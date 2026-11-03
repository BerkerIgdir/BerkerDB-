package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.management.ThreadMXBean;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves fetching (READ and WRITE), unpinning, evicting, creating and flushing pages allocate
 * nothing. Same method as {@link DiskManagerZeroAllocTest}: measure this thread's allocated
 * bytes around a warmed-up loop and require exactly 0. The loops reuse one {@link PageGuard},
 * as real callers will.
 *
 * <p>Single-threaded, so page latches are never contended here. A thread that has to wait for
 * a latch makes StampedLock allocate a queue node; that case is not (and can't be) zero.
 */
@Tag("zero-alloc")
class BufferPoolZeroAllocTest {

    private static final int FRAMES = 16;
    private static final int WARMUP = 20_000;
    private static final int MEASURED = 20_000;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @TempDir
    Path dir;

    private DiskManager disk;
    private BufferPool pool;
    private final PageGuard guard = new PageGuard();

    @BeforeEach
    void setUp() {
        disk = new DiskManager();
        disk.openFile(0, dir.resolve("t0"));
        pool = new BufferPool(disk, FRAMES);
    }

    @AfterEach
    void tearDown() {
        pool.close();
        disk.close();
    }

    @Test
    void hitsAllocateNothing() {
        createPages(FRAMES / 2); // all fit: every fetch is a hit
        fetchLoop(WARMUP, FRAMES / 2);
        long before = THREADS.getCurrentThreadAllocatedBytes();
        fetchLoop(MEASURED, FRAMES / 2);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
        assertEquals(0, allocated, "bytes allocated by " + MEASURED + " fetches that hit");
    }

    @Test
    void missesWithDirtyEvictionsAllocateNothing() {
        int pages = FRAMES * 4; // cycling through 4x the frames: nearly every fetch misses
        createPages(pages);
        fetchLoop(WARMUP, pages);
        long before = THREADS.getCurrentThreadAllocatedBytes();
        fetchLoop(MEASURED, pages);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
        assertEquals(0, allocated, "bytes allocated by " + MEASURED + " fetches that miss and evict");
    }

    @Test
    void newPageAllocatesNothing() {
        // Fewer iterations: every call grows the file by 8 KB.
        int warmup = 4_000;
        int measured = 4_000;
        newPageLoop(warmup);
        long before = THREADS.getCurrentThreadAllocatedBytes();
        newPageLoop(measured);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
        assertEquals(0, allocated, "bytes allocated by " + measured + " newPage calls");
        assertEquals(warmup + measured, disk.pageCount(0));
    }

    private void createPages(int count) {
        for (int p = 0; p < count; p++) {
            disk.allocatePage(0);
        }
    }

    /**
     * Fetches pages round-robin, alternating WRITE (change + markDirty) and READ, so both latch
     * modes are measured and evictions also write back.
     */
    private void fetchLoop(int iterations, int pages) {
        for (int i = 0; i < iterations; i++) {
            if ((i & 1) == 0) {
                pool.fetchPage(0, i % pages, LatchMode.WRITE, guard);
                try (guard) {
                    guard.segment().set(ValueLayout.JAVA_INT, 0, i);
                    guard.markDirty();
                }
            } else {
                pool.fetchPage(0, i % pages, LatchMode.READ, guard);
                try (guard) {
                    sink += guard.segment().get(ValueLayout.JAVA_INT, 0);
                }
            }
        }
    }

    /** Keeps the JIT from dropping the reads as unused. */
    private int sink;

    @Test
    void flushAllAllocatesNothing() {
        createPages(FRAMES);
        fetchLoop(WARMUP, FRAMES);
        for (int i = 0; i < 2_000; i++) {
            dirtyAll();
            pool.flushAll();
        }
        long before = THREADS.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < 2_000; i++) {
            dirtyAll();
            pool.flushAll();
        }
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
        assertEquals(0, allocated, "bytes allocated by 2000 flushAll calls over " + FRAMES + " dirty pages");
    }

    private void dirtyAll() {
        for (int p = 0; p < FRAMES; p++) {
            pool.fetchPage(0, p, LatchMode.WRITE, guard);
            try (guard) {
                guard.markDirty();
            }
        }
    }

    private void newPageLoop(int iterations) {
        for (int i = 0; i < iterations; i++) {
            pool.newPage(0, guard);
            try (guard) {
                guard.markDirty();
            }
        }
    }
}
