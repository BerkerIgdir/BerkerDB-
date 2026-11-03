package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sun.management.ThreadMXBean;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Proves inserting, reading and deleting tuples allocate nothing, including the delete that
 * slides other tuples and the overlapping {@code MemorySegment.copy} it uses. Same method as the
 * other zero-alloc tests: this thread's allocated bytes around a warmed-up loop must be 0.
 */
@Tag("zero-alloc")
class SlottedPageZeroAllocTest {

    private static final int WARMUP_ROUNDS = 4;
    private static final int MEASURED = 50_000;

    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private Arena arena;
    private MemorySegment memory;
    private MemorySegment tuple;
    private final SlottedPage page = new SlottedPage();
    private int sink;

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        memory = arena.allocate(PAGE_SIZE, 8);
        tuple = arena.allocate(512);
        tuple.fill((byte) 7);
        page.wrap(memory).init();
    }

    @AfterEach
    void tearDown() {
        arena.close();
    }

    @Test
    void insertReadDeleteAllocateNothing() {
        // Warm up with the same call that is measured, several times. One warmup call wasn't
        // enough: the first full-size call after it still crossed JIT transitions (deopts in the
        // JDK's MemorySegment internals, then recompilation) and allocated ~100 bytes while
        // interpreted or C1 code ran. Every later call measured exactly 0. We claim zero
        // allocation for the steady state, so that is what is measured.
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            churn(MEASURED);
        }
        long before = THREADS.getCurrentThreadAllocatedBytes();
        churn(MEASURED);
        long allocated = THREADS.getCurrentThreadAllocatedBytes() - before;
        assertEquals(0, allocated, "bytes allocated by " + MEASURED + " insert/read/delete rounds");
    }

    /**
     * Keeps the page about half full: inserts tuples of varying size, reads one, and deletes a
     * slot in the middle, so deletes always have tuples below them to move.
     */
    private void churn(int iterations) {
        for (int i = 0; i < iterations; i++) {
            int length = 1 + (i * 37) % 300;
            if (page.insert(tuple, 0, length) == -1 || (i & 3) == 0) {
                int count = page.slotCount();
                for (int s = count / 2; s < count + count / 2; s++) {
                    int slot = s % count;
                    if (page.isLive(slot)) {
                        sink += page.tupleLength(slot) + page.tupleOffset(slot);
                        page.delete(slot);
                        break;
                    }
                }
            }
        }
    }
}
