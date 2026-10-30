package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Stress tests: many threads hammer one DiskManager. A passing run doesn't prove there are
 * no races (races are timing-dependent), but these catch the obvious ones: lost
 * allocations, pages bleeding into each other, and a table dying after an interrupt.
 */
class DiskManagerConcurrencyTest {

    private static final int THREADS = 8;

    @TempDir
    Path dir;

    private DiskManager disk;

    @BeforeEach
    void setUp() {
        disk = new DiskManager();
        disk.openFile(0, dir.resolve("t0"));
    }

    @AfterEach
    void tearDown() {
        disk.close();
    }

    interface Worker {
        void run(int threadIndex) throws Exception;
    }

    /** Starts all threads at the same moment, waits for them, and rethrows the first failure. */
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

    private static void fill(MemorySegment page, int seed) {
        for (int i = 0; i < PAGE_SIZE; i += 4) {
            page.set(ValueLayout.JAVA_INT_UNALIGNED, i, seed * 31 + i);
        }
    }

    @Test
    void concurrentAllocationsNeverHandOutTheSamePage() throws Exception {
        int perThread = 200;
        int[][] got = new int[THREADS][perThread];

        runConcurrently(THREADS, t -> {
            for (int i = 0; i < perThread; i++) {
                got[t][i] = disk.allocatePage(0);
            }
        });

        int total = THREADS * perThread;
        boolean[] seen = new boolean[total];
        for (int[] pages : got) {
            for (int p : pages) {
                assertTrue(p >= 0 && p < total, "page out of range: " + p);
                assertFalse(seen[p], "page handed out twice: " + p);
                seen[p] = true;
            }
        }
        assertEquals(total, disk.pageCount(0));
        assertEquals((long) total * PAGE_SIZE, Files.size(dir.resolve("t0")));
    }

    @Test
    void concurrentReadsAndWritesOnDifferentPagesDontMix() throws Exception {
        int pagesPerThread = 8;
        for (int p = 0; p < THREADS * pagesPerThread; p++) {
            disk.allocatePage(0);
        }

        runConcurrently(THREADS, t -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(PAGE_SIZE);
                MemorySegment in = arena.allocate(PAGE_SIZE);
                for (int iteration = 0; iteration < 300; iteration++) {
                    // Thread t owns pages t, t + THREADS, t + 2*THREADS, ...
                    int pageNo = t + THREADS * (iteration % pagesPerThread);
                    fill(out, t * 1_000_000 + iteration);
                    disk.writePage(0, pageNo, out);
                    disk.readPage(0, pageNo, in);
                    assertEquals(-1, out.mismatch(in), "thread " + t + " page " + pageNo);
                }
            }
        });
    }

    @Test
    void readersNeverSeeAPageBeforeItIsWritten() throws Exception {
        // One thread allocates; the others read any page the page count says exists.
        // That page must already be fully written (all zeros) - never "beyond end of file".
        int pagesToAllocate = 2_000;
        AtomicBoolean done = new AtomicBoolean();

        runConcurrently(THREADS, t -> {
            if (t == 0) {
                for (int i = 0; i < pagesToAllocate; i++) {
                    disk.allocatePage(0);
                }
                done.set(true);
                return;
            }
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment page = arena.allocate(PAGE_SIZE);
                MemorySegment zeros = arena.allocate(PAGE_SIZE);
                while (!done.get()) {
                    int count = disk.pageCount(0);
                    if (count == 0) {
                        continue;
                    }
                    page.fill((byte) 1);
                    disk.readPage(0, ThreadLocalRandom.current().nextInt(count), page);
                    assertEquals(-1, page.mismatch(zeros));
                }
            }
        });
        assertEquals(pagesToAllocate, disk.pageCount(0));
    }

    // ---------------------------------------------------------------- interrupts

    @Test
    void interruptedReadFailsButTheFileStaysUsable() throws IOException {
        disk.allocatePage(0);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(PAGE_SIZE);
            fill(page, 5);
            disk.writePage(0, 0, page);

            Thread.currentThread().interrupt();
            UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> disk.readPage(0, 0, page));
            assertInstanceOf(ClosedByInterruptException.class, e.getCause());
            assertTrue(Thread.interrupted(), "the interrupt flag must survive (this also clears it)");

            // Java closed the channel; the DiskManager must have reopened it.
            page.fill((byte) 0);
            disk.readPage(0, 0, page);
            MemorySegment expected = arena.allocate(PAGE_SIZE);
            fill(expected, 5);
            assertEquals(-1, page.mismatch(expected));
        }
    }

    @Test
    void interruptedAllocationLeavesTheFileConsistent() throws IOException {
        disk.allocatePage(0);
        disk.allocatePage(0);

        Thread.currentThread().interrupt();
        assertThrows(UncheckedIOException.class, () -> disk.allocatePage(0));
        assertTrue(Thread.interrupted());

        assertEquals(2, disk.pageCount(0));
        assertEquals(2L * PAGE_SIZE, Files.size(dir.resolve("t0")), "no partial page left behind");
        assertEquals(2, disk.allocatePage(0));
    }

    @Test
    void oneThreadsInterruptDoesNotBreakOtherThreads() throws Exception {
        for (int p = 0; p < 16; p++) {
            disk.allocatePage(0);
        }
        AtomicBoolean victimFailed = new AtomicBoolean();
        CountDownLatch victimRunning = new CountDownLatch(1);
        Thread[] victim = new Thread[1];

        runConcurrently(THREADS, t -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment page = arena.allocate(PAGE_SIZE);
                if (t == 0) {
                    // The victim reads until an interrupt makes it fail.
                    victim[0] = Thread.currentThread();
                    victimRunning.countDown();
                    try {
                        for (int i = 0; ; i++) {
                            disk.readPage(0, i & 15, page);
                        }
                    } catch (UncheckedIOException e) {
                        assertInstanceOf(ClosedByInterruptException.class, e.getCause());
                        victimFailed.set(true);
                    }
                } else if (t == 1) {
                    victimRunning.await();
                    Thread.sleep(20);
                    victim[0].interrupt();
                } else {
                    // Bystanders: their channel gets closed under them, but none of their
                    // calls may fail.
                    for (int i = 0; i < 5_000; i++) {
                        disk.readPage(0, i & 15, page);
                        disk.writePage(0, i & 15, page);
                    }
                }
            }
        });
        assertTrue(victimFailed.get());
    }

    @Test
    void ioOnADeliberatelyClosedFileFailsInsteadOfRetrying() {
        try (Arena arena = Arena.ofConfined()) {
            disk.allocatePage(0);
            disk.closeFile(0);
            MemorySegment page = arena.allocate(PAGE_SIZE);
            assertThrows(IllegalArgumentException.class, () -> disk.readPage(0, 0, page));
        }
    }
}
