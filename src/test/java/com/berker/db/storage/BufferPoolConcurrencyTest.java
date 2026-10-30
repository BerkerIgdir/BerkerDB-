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
                pool.fetchPage(0, pageNo, guard);
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
                pool.fetchPage(0, pageNo, guard);
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
