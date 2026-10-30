package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BufferPoolTest {

    @TempDir
    Path dir;

    private Arena arena;
    private DiskManager disk;
    private BufferPool pool;
    private MemorySegment scratch;

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        scratch = arena.allocate(PAGE_SIZE);
        disk = new DiskManager();
        disk.openFile(0, dir.resolve("t0"));
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.checkInvariants();
            pool.close();
        }
        disk.close();
        arena.close();
    }

    private BufferPool pool(int frames) {
        pool = new BufferPool(disk, frames);
        return pool;
    }

    /** Creates {@code count} pages on disk; page p starts with the int p * 100 (written behind the pool's back). */
    private void diskPages(int count) {
        for (int p = 0; p < count; p++) {
            disk.allocatePage(0);
            setDisk(p, p * 100);
        }
    }

    private void setDisk(int pageNo, int value) {
        scratch.fill((byte) 0);
        scratch.set(ValueLayout.JAVA_INT, 0, value);
        disk.writePage(0, pageNo, scratch);
    }

    private int onDisk(int pageNo) {
        disk.readPage(0, pageNo, scratch);
        return scratch.get(ValueLayout.JAVA_INT, 0);
    }

    private static int firstInt(PageGuard guard) {
        return guard.segment().get(ValueLayout.JAVA_INT, 0);
    }

    // ---------------------------------------------------------------- fetching

    @Test
    void fetchReadsThePageFromDisk() {
        diskPages(3);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 2, guard);
        try (guard) {
            assertEquals(200, firstInt(guard));
            assertEquals(0, guard.fileId());
            assertEquals(2, guard.pageNo());
            assertEquals(1, pool.pinCount(0, 2));
        }
        assertEquals(0, pool.pinCount(0, 2), "closing the guard unpins");
    }

    @Test
    void secondFetchIsServedFromMemory() {
        diskPages(1);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.close();

        setDisk(0, 999); // change the disk behind the pool's back
        pool.fetchPage(0, 0, guard);
        try (guard) {
            assertEquals(0, firstInt(guard), "a hit must not read the disk again");
        }
        assertEquals(1, pool.residentPageCount());
    }

    @Test
    void samePageTwiceIsOneFrameWithTwoPins() {
        diskPages(1);
        pool(4);
        PageGuard a = new PageGuard();
        PageGuard b = new PageGuard();
        pool.fetchPage(0, 0, a);
        pool.fetchPage(0, 0, b);
        assertEquals(2, pool.pinCount(0, 0));
        assertEquals(a.segment().address(), b.segment().address(), "same frame");
        a.close();
        assertEquals(1, pool.pinCount(0, 0));
        b.close();
        assertEquals(0, pool.pinCount(0, 0));
    }

    @Test
    void newPageIsZeroedAndAddedToTheFile() {
        diskPages(2);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.newPage(0, guard);
        try (guard) {
            assertEquals(2, guard.pageNo());
            assertEquals(-1, guard.segment().mismatch(arena.allocate(PAGE_SIZE)), "all zeros");
        }
        assertEquals(3, disk.pageCount(0));
    }

    @Test
    void newPageInAReusedFrameIsZeroedToo() {
        diskPages(1);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard); // frame now holds non-zero bytes
        guard.close();
        pool.newPage(0, guard);
        try (guard) {
            assertEquals(-1, guard.segment().mismatch(arena.allocate(PAGE_SIZE)));
        }
    }

    // ---------------------------------------------------------------- eviction

    @Test
    void dirtyPageIsWrittenBackWhenEvicted() {
        diskPages(2);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        try (guard) {
            guard.segment().set(ValueLayout.JAVA_INT, 0, 7777);
            guard.markDirty();
        }
        assertTrue(pool.isDirty(0, 0));
        assertEquals(0, onDisk(0), "not written yet: write-back is lazy");

        pool.fetchPage(0, 1, guard); // only one frame: evicts page 0
        guard.close();
        assertEquals(7777, onDisk(0));
        assertEquals(-1, pool.pinCount(0, 0), "page 0 is no longer in the pool");
    }

    @Test
    void cleanPageIsNotWrittenWhenEvicted() {
        diskPages(2);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.close();

        setDisk(0, 555); // if eviction wrote the stale frame, this would be overwritten
        pool.fetchPage(0, 1, guard);
        guard.close();
        assertEquals(555, onDisk(0));
    }

    @Test
    void evictsTheLeastRecentlyUsedPage() {
        diskPages(3);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.close();
        pool.fetchPage(0, 1, guard);
        guard.close();
        pool.fetchPage(0, 0, guard); // page 0 used again: page 1 is now least recent
        guard.close();

        pool.fetchPage(0, 2, guard);
        guard.close();
        assertEquals(0, pool.pinCount(0, 0), "page 0 stays");
        assertEquals(-1, pool.pinCount(0, 1), "page 1 was evicted");
        assertEquals(0, pool.pinCount(0, 2));
    }

    @Test
    void pinnedPagesAreNeverEvictedAndAFullPoolThrows() {
        diskPages(3);
        pool(2);
        PageGuard a = new PageGuard();
        PageGuard b = new PageGuard();
        PageGuard c = new PageGuard();
        pool.fetchPage(0, 0, a);
        pool.fetchPage(0, 1, b);

        BufferPoolFullException e = assertThrows(BufferPoolFullException.class, () -> pool.fetchPage(0, 2, c));
        assertTrue(e.getMessage().contains("no available buffer"), e.getMessage());
        assertFalse(c.isHeld());
        assertThrows(BufferPoolFullException.class, () -> pool.newPage(0, c));
        assertEquals(3, disk.pageCount(0), "a refused newPage must not grow the file");
        assertEquals(1, pool.pinCount(0, 0));
        assertEquals(1, pool.pinCount(0, 1));

        // Once a page is unpinned, the pool works again.
        a.close();
        pool.fetchPage(0, 2, c);
        c.close();
        b.close();
    }

    // ---------------------------------------------------------------- flushing

    @Test
    void flushPageWritesAndCleans() {
        diskPages(1);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.segment().set(ValueLayout.JAVA_INT, 0, 42);
        guard.markDirty();
        guard.close();

        pool.flushPage(0, 0);
        assertEquals(42, onDisk(0));
        assertFalse(pool.isDirty(0, 0));
        pool.flushPage(0, 5); // not in the pool: nothing to do
    }

    @Test
    void flushFileOnlyWritesThatFile() {
        diskPages(1);
        disk.openFile(1, dir.resolve("t1"));
        disk.allocatePage(1);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.markDirty();
        guard.close();
        pool.fetchPage(1, 0, guard);
        guard.markDirty();
        guard.close();

        pool.flushFile(1);
        assertTrue(pool.isDirty(0, 0));
        assertFalse(pool.isDirty(1, 0));

        pool.flushAll();
        assertFalse(pool.isDirty(0, 0));
    }

    @Test
    void dirtyFlagSurvivesWhenAnotherHolderClosesClean() {
        diskPages(1);
        pool(2);
        PageGuard writer = new PageGuard();
        PageGuard reader = new PageGuard();
        pool.fetchPage(0, 0, writer);
        pool.fetchPage(0, 0, reader);
        writer.markDirty();
        writer.close();
        reader.close(); // closing clean must not clear the writer's dirty mark
        assertTrue(pool.isDirty(0, 0));
    }

    // ---------------------------------------------------------------- guards and errors

    @Test
    void fetchingIntoAHeldGuardThrowsWithoutPinning() {
        diskPages(2);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        assertThrows(IllegalStateException.class, () -> pool.fetchPage(0, 1, guard));
        assertEquals(-1, pool.pinCount(0, 1), "page 1 must not have been pinned");
        assertEquals(0, guard.pageNo(), "guard still holds page 0");
        guard.close();
    }

    @Test
    void closedGuardRefusesAccessAndClosingTwiceIsSafe() {
        diskPages(1);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.close();
        guard.close();
        assertEquals(0, pool.pinCount(0, 0), "second close must not unpin again");
        assertThrows(IllegalStateException.class, guard::segment);
        assertThrows(IllegalStateException.class, guard::markDirty);
    }

    @Test
    void badRequestsDontCostAFrame() {
        diskPages(1);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        guard.close();

        assertThrows(IndexOutOfBoundsException.class, () -> pool.fetchPage(0, 1, guard));
        assertThrows(IndexOutOfBoundsException.class, () -> pool.fetchPage(0, -1, guard));
        assertThrows(IllegalArgumentException.class, () -> pool.fetchPage(9, 0, guard));
        assertEquals(0, pool.pinCount(0, 0), "page 0 must still be in the pool");
    }

    @Test
    void unpinningAnUnpinnedFrameThrows() {
        diskPages(1);
        pool(2);
        Frame frame = pool.pin(0, 0);
        pool.unpin(frame, false);
        assertThrows(IllegalStateException.class, () -> pool.unpin(frame, false));
    }

    @Test
    void closeRefusesWhilePagesArePinned() {
        diskPages(1);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, guard);
        IllegalStateException e = assertThrows(IllegalStateException.class, pool::close);
        assertTrue(e.getMessage().contains("still pinned"), e.getMessage());
        guard.close();
        pool.close();
        assertThrows(IllegalStateException.class, () -> pool.fetchPage(0, 0, guard));
        pool.close(); // idempotent
        pool = null;  // already closed; skip tearDown's checks
    }

    @Test
    void rejectsBadFrameCount() {
        assertThrows(IllegalArgumentException.class, () -> new BufferPool(disk, 0));
    }

    @Test
    void framesAreAligned() {
        pool(3);
        PageGuard guard = new PageGuard();
        pool.newPage(0, guard);
        try (guard) {
            assertEquals(0, guard.segment().address() % 4096);
        }
    }
}
