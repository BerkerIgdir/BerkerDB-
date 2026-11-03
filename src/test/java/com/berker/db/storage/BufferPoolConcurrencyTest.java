package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Many threads share one small pool, so frames are evicted and reused constantly. As with the
 * DiskManager stress tests, passing doesn't prove there are no races, but these catch the
 * classic ones: a page loaded into two frames (updates get lost), a page evicted while in use
 * (its contents change under the holder), and bookkeeping that drifts out of sync.
 */
class BufferPoolConcurrencyTest {

    private static final int THREADS = 8;
    private static final int FRAMES = 16;   // >= THREADS: each thread pins at most one page

    @TempDir
    Path dir;

    private DiskManager disk;
    private BufferPool pool;

    @BeforeEach
    void setUp() {
        disk = new DiskManager();
        disk.openFile(0, dir.resolve("t0"));
        pool = new BufferPool(disk, FRAMES);
    }

    @AfterEach
    void tearDown() {
        pool.checkInvariants();
        pool.close();
        disk.close();
    }

    interface Worker {
        void run(int threadIndex) throws Exception;
    }

    private static void runConcurrently(int threads, Worker worker) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        List<Thread> started = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int index = t;
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    worker.run(index);
                } catch (Throwable e) {
                    failures.add(e);
                }
            }, "worker-" + t);
            thread.start();
            started.add(thread);
        }
        start.countDown();
        for (Thread thread : started) {
            thread.join(TimeUnit.SECONDS.toMillis(60));
            assertFalse(thread.isAlive(), thread.getName() + " did not finish");
        }
        if (!failures.isEmpty()) {
            AssertionError error = new AssertionError(failures.size() + " worker(s) failed");
            failures.forEach(error::addSuppressed);
            throw error;
        }
    }

    /**
     * Each thread owns its own pages and keeps a counter in each, incrementing it on every
     * visit. The pool holds far fewer frames than there are pages, so a page is usually
     * evicted (and written back) between two visits. If a write-back were lost, or a page
     * ended up in two frames, the counter the thread finds would differ from the one it
     * expects.
     */
    @Test
    void updatesSurviveConstantEviction() throws Exception {
        int pagesPerThread = 16;
        int visits = 3_000;
        for (int p = 0; p < THREADS * pagesPerThread; p++) {
            disk.allocatePage(0);
        }
        int[][] expected = new int[THREADS][pagesPerThread];

        runConcurrently(THREADS, t -> {
            PageGuard guard = new PageGuard();
            for (int i = 0; i < visits; i++) {
                int k = ThreadLocalRandom.current().nextInt(pagesPerThread);
                int pageNo = t + THREADS * k;   // thread t owns pages t, t+8, t+16, ...
                pool.fetchPage(0, pageNo, LatchMode.WRITE, guard);
                try (guard) {
                    MemorySegment page = guard.segment();
                    int found = page.get(ValueLayout.JAVA_INT, 0);
                    assertEquals(expected[t][k], found, "thread " + t + " page " + pageNo + " visit " + i);
                    page.set(ValueLayout.JAVA_INT, 0, found + 1);
                    guard.markDirty();
                    expected[t][k] = found + 1;
                }
            }
        });

        pool.flushAll();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            for (int t = 0; t < THREADS; t++) {
                for (int k = 0; k < pagesPerThread; k++) {
                    disk.readPage(0, t + THREADS * k, page);
                    assertEquals(expected[t][k], page.get(ValueLayout.JAVA_INT, 0), "page " + (t + THREADS * k) + " on disk");
                }
            }
        }
    }

    /**
     * All threads read random pages from one shared set, so the same page is often held by
     * several threads, and frames change pages constantly. Each page carries its own page
     * number at the start and the end; finding anything else means a thread got the wrong
     * frame, or a frame was reused while someone still held it.
     */
    @Test
    void sharedReadersAlwaysSeeTheRightPage() throws Exception {
        int pages = 64;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            for (int p = 0; p < pages; p++) {
                disk.allocatePage(0);
                page.set(ValueLayout.JAVA_INT, 0, p);
                page.set(ValueLayout.JAVA_INT, PAGE_SIZE - 4, p);
                disk.writePage(0, p, page);
            }
        }

        runConcurrently(THREADS, t -> {
            PageGuard guard = new PageGuard();
            for (int i = 0; i < 5_000; i++) {
                int pageNo = ThreadLocalRandom.current().nextInt(pages);
                pool.fetchPage(0, pageNo, LatchMode.READ, guard);
                try (guard) {
                    MemorySegment page = guard.segment();
                    assertEquals(pageNo, page.get(ValueLayout.JAVA_INT, 0));
                    Thread.onSpinWait(); // hold the page a moment so others can try to steal it
                    assertEquals(pageNo, page.get(ValueLayout.JAVA_INT, PAGE_SIZE - 4));
                    assertEquals(pageNo, guard.pageNo());
                }
            }
        });

        for (int p = 0; p < pages; p++) {
            int pins = pool.pinCount(0, p);
            assertEquals(true, pins == -1 || pins == 0, "page " + p + " still pinned: " + pins);
        }
    }

    /**
     * The case v1 didn't protect: all threads change the SAME page. Each visit reads a counter
     * and writes it back plus one. Without an exclusive latch two threads read the same value
     * and one increment is lost (the "lost update" at byte level). A flusher writes the page
     * back the whole time, so flushing is exercised against live writers too.
     */
    @Test
    void writersOnOnePageNeverLoseAnIncrement() throws Exception {
        int increments = 5_000;
        disk.allocatePage(0);
        for (int p = 1; p < FRAMES * 2; p++) {
            disk.allocatePage(0); // other pages, so eviction churns around the hot page
        }
        AtomicBoolean writersDone = new AtomicBoolean();
        AtomicInteger writersLeft = new AtomicInteger(THREADS - 1);

        runConcurrently(THREADS, t -> {
            if (t == 0) {
                while (!writersDone.get()) {
                    pool.flushAll();
                }
                return;
            }
            PageGuard guard = new PageGuard();
            try {
                for (int i = 0; i < increments; i++) {
                    pool.fetchPage(0, 0, LatchMode.WRITE, guard);
                    try (guard) {
                        MemorySegment page = guard.segment();
                        int value = page.get(ValueLayout.JAVA_INT, 0);
                        Thread.onSpinWait(); // widen the window between read and write
                        page.set(ValueLayout.JAVA_INT, 0, value + 1);
                        guard.markDirty();
                    }
                    pool.fetchPage(0, 1 + ThreadLocalRandom.current().nextInt(FRAMES * 2 - 1), LatchMode.READ, guard);
                    guard.close();
                }
            } finally {
                if (writersLeft.decrementAndGet() == 0) {
                    writersDone.set(true);
                }
            }
        });

        pool.flushAll();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            disk.readPage(0, 0, page);
            assertEquals((THREADS - 1) * increments, page.get(ValueLayout.JAVA_INT, 0));
        }
    }

    /**
     * Writers keep two fields of a page equal, setting them one after the other; readers check
     * them under READ. A reader that sees them differ caught a writer halfway: the shared latch
     * didn't exclude the writer.
     */
    @Test
    void readersNeverSeeAHalfDoneChange() throws Exception {
        int hotPages = 4;
        for (int p = 0; p < hotPages; p++) {
            disk.allocatePage(0);
        }
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger writersLeft = new AtomicInteger(2);

        runConcurrently(THREADS, t -> {
            PageGuard guard = new PageGuard();
            if (t < 2) {
                try {
                    for (int i = 1; i <= 5_000; i++) {
                        pool.fetchPage(0, i % hotPages, LatchMode.WRITE, guard);
                        try (guard) {
                            MemorySegment page = guard.segment();
                            page.set(ValueLayout.JAVA_INT, 0, i);
                            Thread.onSpinWait();
                            page.set(ValueLayout.JAVA_INT, PAGE_SIZE - 4, i);
                            guard.markDirty();
                        }
                    }
                } finally {
                    if (writersLeft.decrementAndGet() == 0) {
                        stop.set(true);
                    }
                }
                return;
            }
            while (!stop.get()) {
                pool.fetchPage(0, ThreadLocalRandom.current().nextInt(hotPages), LatchMode.READ, guard);
                try (guard) {
                    MemorySegment page = guard.segment();
                    int a = page.get(ValueLayout.JAVA_INT, 0);
                    Thread.onSpinWait();
                    int b = page.get(ValueLayout.JAVA_INT, PAGE_SIZE - 4);
                    assertEquals(a, b, "reader saw a half-done change on page " + guard.pageNo());
                }
            }
        });
    }

    /**
     * Aims at the window in the last unpin: the count is already 0 but the frame isn't in the
     * replacer yet. Half the threads keep reading one hot page, so its count keeps dropping to
     * 0 and rising again; the other half change cold pages through a pool that is too small, so
     * an evictor is always looking for a victim. If a re-pinned frame got into the replacer, it
     * would be evicted while held (the reader sees another page), and if the hot page's changes
     * were dropped, the counter would come back short. The invariant check in tearDown catches
     * a frame left both free and in the replacer.
     */
    @Test
    void lastUnpinRacesWithRepinsAndEvictions() throws Exception {
        int coldPages = FRAMES * 2;
        int visits = 5_000;
        int hotWriters = THREADS / 2;
        for (int p = 0; p <= coldPages; p++) {
            disk.allocatePage(0); // page 0 is the hot one
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            for (int p = 1; p <= coldPages; p++) {
                page.set(ValueLayout.JAVA_INT, 4, p);
                disk.writePage(0, p, page);
            }
        }

        runConcurrently(THREADS, t -> {
            PageGuard guard = new PageGuard();
            for (int i = 0; i < visits; i++) {
                if (t < hotWriters) {
                    // Mostly READ, with a WRITE now and then that has to survive eviction.
                    LatchMode mode = (i & 7) == 0 ? LatchMode.WRITE : LatchMode.READ;
                    pool.fetchPage(0, 0, mode, guard);
                    try (guard) {
                        assertEquals(0, guard.pageNo());
                        if (mode == LatchMode.WRITE) {
                            MemorySegment page = guard.segment();
                            page.set(ValueLayout.JAVA_INT, 0, page.get(ValueLayout.JAVA_INT, 0) + 1);
                            guard.markDirty();
                        }
                    }
                } else {
                    int pageNo = 1 + ThreadLocalRandom.current().nextInt(coldPages);
                    pool.fetchPage(0, pageNo, LatchMode.READ, guard);
                    try (guard) {
                        assertEquals(pageNo, guard.segment().get(ValueLayout.JAVA_INT, 4), "cold page " + pageNo);
                    }
                }
            }
        });

        int pins = pool.pinCount(0, 0);
        assertEquals(true, pins == -1 || pins == 0, "hot page still pinned: " + pins);
        pool.flushAll();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            disk.readPage(0, 0, page);
            assertEquals(hotWriters * (visits / 8), page.get(ValueLayout.JAVA_INT, 0), "hot page increments");
        }
    }

    /** Threads create pages concurrently: every new page number must be handed out once, in its own frame. */
    @Test
    void concurrentNewPagesAreDistinct() throws Exception {
        int perThread = 200;
        int[][] got = new int[THREADS][perThread];

        runConcurrently(THREADS, t -> {
            PageGuard guard = new PageGuard();
            for (int i = 0; i < perThread; i++) {
                pool.newPage(0, guard);
                try (guard) {
                    got[t][i] = guard.pageNo();
                    // Stamp it, so a page that ends up shared between frames shows up below.
                    guard.segment().set(ValueLayout.JAVA_INT, 0, guard.pageNo());
                    guard.markDirty();
                }
            }
        });

        int total = THREADS * perThread;
        boolean[] seen = new boolean[total];
        for (int[] pages : got) {
            for (int p : pages) {
                assertFalse(seen[p], "page handed out twice: " + p);
                seen[p] = true;
            }
        }
        pool.flushAll();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            for (int p = 0; p < total; p++) {
                disk.readPage(0, p, page);
                assertEquals(p, page.get(ValueLayout.JAVA_INT, 0), "page " + p + " on disk");
            }
        }
    }
}
