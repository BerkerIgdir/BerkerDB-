package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves the hot-path methods allocate nothing.
 *
 * <p>How: the JVM counts the bytes each thread allocates. We read that counter, run the hot
 * path many times, read it again, and require the difference to be exactly 0. Inside the
 * measured loops the test itself uses only ints and pre-made buffers, so any byte counted
 * comes from DiskManager or the JDK code it calls.
 *
 * <p>Tagged {@code zero-alloc}, so Maven runs it in a separate JVM under Epsilon GC (see
 * pom.xml). Epsilon never collects, so nothing can be freed behind our back.
 *
 * <p>We warm up first so the measurement covers JIT-compiled code, which is what runs in
 * production. Our own code doesn't rely on the JIT removing allocations, though.
 */
@Tag("zero-alloc")
class DiskManagerZeroAllocTest {

    private static final int FRAMES = 16;
    private static final int PAGES = 64;
    private static final int WARMUP = 20_000;
    private static final int MEASURED = 20_000;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @TempDir
    Path dir;

    private Arena arena;
    private DiskManager disk;
    // What the BufferPool will do: one big segment, one ByteBuffer view per frame, created once.
    private ByteBuffer[] frames;

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        MemorySegment pool = arena.allocate((long) FRAMES * PAGE_SIZE, PAGE_SIZE);
        frames = new ByteBuffer[FRAMES];
        for (int f = 0; f < FRAMES; f++) {
            frames[f] = pool.asSlice((long) f * PAGE_SIZE, PAGE_SIZE).asByteBuffer();
        }
        disk = new DiskManager();
    }

    @AfterEach
    void tearDown() {
        disk.close();
        arena.close();
    }

    /** Guards the pom setup: if the Epsilon argLine gets lost, this fails instead of passing silently. */
    @Test
    void runsUnderEpsilonGc() {
        assertTrue(ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-XX:+UseEpsilonGC"),
                "zero-alloc tests must run with -XX:+UseEpsilonGC (run them via Maven, or add the flag to the IDE run config)");
    }

    @Test
    void allocationCounterItselfAllocatesNothing() {
        long before = THREADS.getCurrentThreadAllocatedBytes();
        long after = THREADS.getCurrentThreadAllocatedBytes();
        assertEquals(0, after - before, "the measurement would be meaningless otherwise");
    }

    @Test
    void readAndWritePageAllocateNothing() {
        disk.openFile(0, dir.resolve("t0"));
        for (int p = 0; p < PAGES; p++) {
            disk.allocatePage(0);
        }

        readWriteLoop(WARMUP);
        long before = THREADS.getCurrentThreadAllocatedBytes();
        readWriteLoop(MEASURED);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;

        assertEquals(0, allocated, "bytes allocated by " + MEASURED + " read+write pairs");
    }

    @Test
    void allocatePageAllocatesNothing() {
        // Fewer iterations: every call grows the file by 8 KB.
        int warmup = 4_000;
        int measured = 4_000;
        disk.openFile(0, dir.resolve("t0"));

        allocateLoop(warmup);
        long before = THREADS.getCurrentThreadAllocatedBytes();
        allocateLoop(measured);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;

        assertEquals(0, allocated, "bytes allocated by " + measured + " allocatePage calls");
        assertEquals(warmup + measured, disk.pageCount(0));
    }

    private void readWriteLoop(int iterations) {
        for (int i = 0; i < iterations; i++) {
            ByteBuffer frame = frames[i & (FRAMES - 1)];
            int pageNo = i & (PAGES - 1);
            disk.readPage(0, pageNo, frame);
            disk.writePage(0, pageNo, frame);
        }
    }

    private void allocateLoop(int iterations) {
        for (int i = 0; i < iterations; i++) {
            disk.allocatePage(0);
        }
    }
}
