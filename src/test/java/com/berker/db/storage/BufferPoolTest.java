package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static com.berker.db.storage.LatchMode.READ;
import static com.berker.db.storage.LatchMode.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BufferPoolTest {

    /** How long a blocked thread gets to (wrongly) get through before we call it blocked. */
    private static final long BLOCKED_MILLIS = 200;

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

    /** Runs {@code action} in another thread; the returned latch opens when it has finished. */
    private static CountDownLatch inThread(AtomicReference<Throwable> failure, Runnable action) {
        CountDownLatch finished = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable e) {
                failure.set(e);
            } finally {
                finished.countDown();
            }
        });
        thread.setDaemon(true);
        thread.start();
        return finished;
    }

    // ---------------------------------------------------------------- fetching

    @Test
    void fetchReadsThePageFromDisk() {
        diskPages(3);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 2, READ, guard);
        try (guard) {
            assertEquals(200, firstInt(guard));
            assertEquals(0, guard.fileId());
            assertEquals(2, guard.pageNo());
            assertEquals(READ, guard.mode());
            assertEquals(1, pool.pinCount(0, 2));
        }
        assertEquals(0, pool.pinCount(0, 2), "closing the guard unpins");
    }

    @Test
    void secondFetchIsServedFromMemory() {
        diskPages(1);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, READ, guard);
        guard.close();

        setDisk(0, 999); // change the disk behind the pool's back
        pool.fetchPage(0, 0, READ, guard);
        try (guard) {
            assertEquals(0, firstInt(guard), "a hit must not read the disk again");
        }
        assertEquals(1, pool.residentPageCount());
    }

    @Test
    void readGuardsShareAPage() {
        diskPages(1);
        pool(4);
        PageGuard a = new PageGuard();
        PageGuard b = new PageGuard();
        pool.fetchPage(0, 0, READ, a);
        pool.fetchPage(0, 0, READ, b); // same thread, both READ: allowed, no deadlock
        assertEquals(2, pool.pinCount(0, 0));
        assertEquals(a.segment().address(), b.segment().address(), "same frame");
        a.close();
        assertEquals(1, pool.pinCount(0, 0));
        b.close();
        assertEquals(0, pool.pinCount(0, 0));
    }

    /**
     * Closing a guard while others still hold the page skips the pool latch (and the replacer),
     * so this checks that the page only becomes evictable at the very last unpin.
     */
    @Test
    void onlyTheLastUnpinMakesAPageEvictable() {
        diskPages(2);
        pool(1);
        PageGuard a = new PageGuard();
        PageGuard b = new PageGuard();
        PageGuard other = new PageGuard();
        pool.fetchPage(0, 0, READ, a);
        pool.fetchPage(0, 0, READ, b);
        a.close();
        assertThrows(BufferPoolFullException.class, () -> pool.fetchPage(0, 1, READ, other),
                "page 0 is still held by b, so its frame must not be taken");
        b.close();
        pool.fetchPage(0, 1, READ, other); // now page 0 can be evicted
        try (other) {
            assertEquals(100, firstInt(other));
        }
        assertEquals(-1, pool.pinCount(0, 0), "page 0 was evicted");
    }

    @Test
    void newPageIsZeroedWriteLatchedAndAddedToTheFile() {
        diskPages(2);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.newPage(0, guard);
        try (guard) {
            assertEquals(2, guard.pageNo());
            assertEquals(WRITE, guard.mode());
            assertEquals(-1, guard.segment().mismatch(arena.allocate(PAGE_SIZE)), "all zeros");
            guard.markDirty(); // allowed: it's a WRITE guard
        }
        assertEquals(3, disk.pageCount(0));
    }

    @Test
    void newPageInAReusedFrameIsZeroedToo() {
        diskPages(1);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, READ, guard); // frame now holds non-zero bytes
        guard.close();
        pool.newPage(0, guard);
        try (guard) {
            assertEquals(-1, guard.segment().mismatch(arena.allocate(PAGE_SIZE)));
        }
    }

    // ---------------------------------------------------------------- read vs write

    @Test
    void readGuardIsReadOnly() {
        diskPages(1);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, READ, guard);
        try (guard) {
            assertThrows(IllegalArgumentException.class, () -> guard.segment().set(ValueLayout.JAVA_INT, 0, 1));
            assertThrows(IllegalStateException.class, guard::markDirty);
        }
        assertFalse(pool.isDirty(0, 0));
    }

    @Test
    void writeGuardBlocksReadersUntilClosed() throws Exception {
        diskPages(1);
        pool(4);
        PageGuard writer = new PageGuard();
        pool.fetchPage(0, 0, WRITE, writer);
        writer.segment().set(ValueLayout.JAVA_INT, 0, 1234);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        int[] seen = new int[1];
        CountDownLatch readerDone = inThread(failure, () -> {
            PageGuard reader = new PageGuard();
            pool.fetchPage(0, 0, READ, reader);
            try (reader) {
                seen[0] = firstInt(reader);
            }
        });

        assertFalse(readerDone.await(BLOCKED_MILLIS, TimeUnit.MILLISECONDS), "reader must wait for the writer");
        writer.segment().set(ValueLayout.JAVA_INT, 0, 5678); // still changing: reader must not see 1234
        writer.close();
        assertTrue(readerDone.await(10, TimeUnit.SECONDS));
        assertEquals(null, failure.get());
        assertEquals(5678, seen[0], "reader sees the finished change only");
    }

    @Test
    void readGuardBlocksWritersUntilClosed() throws Exception {
        diskPages(1);
        pool(4);
        PageGuard reader = new PageGuard();
        pool.fetchPage(0, 0, READ, reader);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch writerDone = inThread(failure, () -> {
            PageGuard writer = new PageGuard();
            pool.fetchPage(0, 0, WRITE, writer);
            try (writer) {
                writer.segment().set(ValueLayout.JAVA_INT, 0, 42);
                writer.markDirty();
            }
        });

        assertFalse(writerDone.await(BLOCKED_MILLIS, TimeUnit.MILLISECONDS), "writer must wait for the reader");
        assertEquals(0, firstInt(reader), "page unchanged while we read it");
        reader.close();
        assertTrue(writerDone.await(10, TimeUnit.SECONDS));
        assertEquals(null, failure.get());
        assertTrue(pool.isDirty(0, 0));
    }

    @Test
    void fetchingAPageThisThreadHoldsWithWriteThrowsInsteadOfDeadlocking() {
        diskPages(1);
        pool(4);
        PageGuard writer = new PageGuard();
        PageGuard again = new PageGuard();
        pool.fetchPage(0, 0, WRITE, writer);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> pool.fetchPage(0, 0, READ, again));
        assertTrue(e.getMessage().contains("not reentrant"), e.getMessage());
        assertFalse(again.isHeld());
        assertEquals(1, pool.pinCount(0, 0), "the failed fetch must not leave a pin behind");
        writer.close();
    }

    @Test
    void nullModeIsRejected() {
        diskPages(1);
        pool(2);
        assertThrows(NullPointerException.class, () -> pool.fetchPage(0, 0, null, new PageGuard()));
        assertEquals(-1, pool.pinCount(0, 0));
    }

    // ---------------------------------------------------------------- eviction

    @Test
    void dirtyPageIsWrittenBackWhenEvicted() {
        diskPages(2);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, WRITE, guard);
        try (guard) {
            guard.segment().set(ValueLayout.JAVA_INT, 0, 7777);
            guard.markDirty();
        }
        assertTrue(pool.isDirty(0, 0));
        assertEquals(0, onDisk(0), "not written yet: write-back is lazy");

        pool.fetchPage(0, 1, READ, guard); // only one frame: evicts page 0
        guard.close();
        assertEquals(7777, onDisk(0));
        assertEquals(-1, pool.pinCount(0, 0), "page 0 is no longer in the pool");
    }

    @Test
    void cleanPageIsNotWrittenWhenEvicted() {
        diskPages(2);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, WRITE, guard); // WRITE but never marked dirty
        guard.close();

        setDisk(0, 555); // if eviction wrote the stale frame, this would be overwritten
        pool.fetchPage(0, 1, READ, guard);
        guard.close();
        assertEquals(555, onDisk(0));
    }

    @Test
    void evictsTheLeastRecentlyUsedPage() {
        diskPages(3);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, READ, guard);
        guard.close();
        pool.fetchPage(0, 1, READ, guard);
        guard.close();
        pool.fetchPage(0, 0, READ, guard); // page 0 used again: page 1 is now least recent
        guard.close();

        pool.fetchPage(0, 2, READ, guard);
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
        pool.fetchPage(0, 0, READ, a);
        pool.fetchPage(0, 1, READ, b);

        BufferPoolFullException e = assertThrows(BufferPoolFullException.class, () -> pool.fetchPage(0, 2, READ, c));
        assertTrue(e.getMessage().contains("no available buffer"), e.getMessage());
        assertFalse(c.isHeld());
        assertThrows(BufferPoolFullException.class, () -> pool.newPage(0, c));
        assertEquals(3, disk.pageCount(0), "a refused newPage must not grow the file");
        assertEquals(1, pool.pinCount(0, 0));
        assertEquals(1, pool.pinCount(0, 1));

        // Once a page is unpinned, the pool works again.
        a.close();
        pool.fetchPage(0, 2, READ, c);
        c.close();
        b.close();
    }

    // ---------------------------------------------------------------- flushing

    @Test
    void flushPageWritesAndCleans() {
        diskPages(1);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, WRITE, guard);
        guard.segment().set(ValueLayout.JAVA_INT, 0, 42);
        guard.markDirty();
        guard.close();

        pool.flushPage(0, 0);
        assertEquals(42, onDisk(0));
        assertFalse(pool.isDirty(0, 0));
        assertEquals(0, pool.pinCount(0, 0), "flushing must not leave a pin behind");
        pool.flushPage(0, 5); // not in the pool: nothing to do
    }

    @Test
    void flushFileOnlyWritesThatFile() {
        diskPages(1);
        disk.openFile(1, dir.resolve("t1"));
        disk.allocatePage(1);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, WRITE, guard);
        guard.markDirty();
        guard.close();
        pool.fetchPage(1, 0, WRITE, guard);
        guard.markDirty();
        guard.close();

        pool.flushFile(1);
        assertTrue(pool.isDirty(0, 0));
        assertFalse(pool.isDirty(1, 0));

        pool.flushAll();
        assertFalse(pool.isDirty(0, 0));
    }

    @Test
    void flushWaitsForAWriterToFinish() throws Exception {
        diskPages(1);
        pool(4);
        PageGuard writer = new PageGuard();
        pool.fetchPage(0, 0, WRITE, writer);
        writer.segment().set(ValueLayout.JAVA_INT, 0, 1);
        writer.markDirty();
        writer.close(); // the page is dirty now

        pool.fetchPage(0, 0, WRITE, writer); // a second change starts
        writer.segment().set(ValueLayout.JAVA_INT, 0, 2);

        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch flushDone = inThread(failure, () -> pool.flushPage(0, 0));
        assertFalse(flushDone.await(BLOCKED_MILLIS, TimeUnit.MILLISECONDS), "flush must not write a half-done change");

        writer.segment().set(ValueLayout.JAVA_INT, 0, 3); // change finished
        writer.markDirty();
        writer.close();
        assertTrue(flushDone.await(10, TimeUnit.SECONDS));
        assertEquals(null, failure.get());
        assertEquals(3, onDisk(0));
    }

    @Test
    void flushingAPageThisThreadHoldsWithWriteThrows() {
        diskPages(1);
        pool(2);
        PageGuard writer = new PageGuard();
        pool.fetchPage(0, 0, WRITE, writer);
        writer.markDirty();
        writer.close();
        pool.fetchPage(0, 0, WRITE, writer);
        assertThrows(IllegalStateException.class, () -> pool.flushPage(0, 0));
        assertEquals(1, pool.pinCount(0, 0), "only the writer's pin remains");
        writer.close();
    }

    @Test
    void dirtyFlagSurvivesALaterCleanClose() {
        diskPages(1);
        pool(2);
        PageGuard writer = new PageGuard();
        PageGuard reader = new PageGuard();
        pool.fetchPage(0, 0, WRITE, writer);
        writer.markDirty();
        writer.close();
        pool.fetchPage(0, 0, READ, reader);
        reader.close(); // closing clean must not clear the writer's dirty mark
        assertTrue(pool.isDirty(0, 0));
    }

    // ---------------------------------------------------------------- guards and errors

    @Test
    void fetchingIntoAHeldGuardThrowsWithoutPinning() {
        diskPages(2);
        pool(4);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, READ, guard);
        assertThrows(IllegalStateException.class, () -> pool.fetchPage(0, 1, READ, guard));
        assertEquals(-1, pool.pinCount(0, 1), "page 1 must not have been pinned");
        assertEquals(0, guard.pageNo(), "guard still holds page 0");
        guard.close();
    }

    @Test
    void closedGuardRefusesAccessAndClosingTwiceIsSafe() {
        diskPages(1);
        pool(2);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, WRITE, guard);
        guard.close();
        guard.close();
        assertEquals(0, pool.pinCount(0, 0), "second close must not unpin again");
        assertThrows(IllegalStateException.class, guard::segment);
        assertThrows(IllegalStateException.class, guard::markDirty);
        assertThrows(IllegalStateException.class, guard::mode);

        // The latch was released: another WRITE fetch gets through.
        pool.fetchPage(0, 0, WRITE, guard);
        guard.close();
    }

    @Test
    void badRequestsDontCostAFrame() {
        diskPages(1);
        pool(1);
        PageGuard guard = new PageGuard();
        pool.fetchPage(0, 0, READ, guard);
        guard.close();

        assertThrows(IndexOutOfBoundsException.class, () -> pool.fetchPage(0, 1, READ, guard));
        assertThrows(IndexOutOfBoundsException.class, () -> pool.fetchPage(0, -1, READ, guard));
        assertThrows(IllegalArgumentException.class, () -> pool.fetchPage(9, 0, READ, guard));
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
        pool.fetchPage(0, 0, READ, guard);
        IllegalStateException e = assertThrows(IllegalStateException.class, pool::close);
        assertTrue(e.getMessage().contains("still pinned"), e.getMessage());
        guard.close();
        pool.close();
        assertThrows(IllegalStateException.class, () -> pool.fetchPage(0, 0, READ, guard));
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
