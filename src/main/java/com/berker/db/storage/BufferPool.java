package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Keeps recently used pages in memory, so most page accesses don't touch the disk.
 *
 * <p>A caller asks for page {@code (fileId, pageNo)}. If it is already in a frame (a hit), the
 * caller gets that frame. If not (a miss), the pool picks a frame to reuse, writes its old page
 * back if it was changed, reads the new page into it, and hands it over. Two ideas from the
 * book shape this:
 * <ul>
 *   <li><b>Pinning.</b> While someone uses a page, it is pinned and can't be evicted. The pin
 *       is taken in the same step as the lookup, so nothing can evict the page in between.</li>
 *   <li><b>Write-back.</b> A changed ("dirty") page goes to disk only when its frame is
 *       reused, or when someone flushes it. Many changes to a hot page cost one write.</li>
 * </ul>
 *
 * <p><b>Memory.</b> One big off-heap segment, split into 8 KB frames. A {@link Frame} object
 * per frame holds the page's metadata, its page latch, and views of its memory. All of it is
 * created in the constructor; afterwards fetching, evicting and flushing allocate nothing
 * (except a latch queue node when a thread actually has to wait for a page latch).
 *
 * <p><b>Finding pages.</b> {@link LongIntHashMap} maps a page key ({@link PageKeys}) to a frame
 * number, and {@link LruReplacer} picks the least recently used unpinned frame to evict.
 * Frames that never held a page, or lost theirs to a failed read, sit on a free list and are
 * used first.
 *
 * <p><b>Two kinds of latch.</b>
 * <ul>
 *   <li>The <b>pool latch</b> (one, global) protects the bookkeeping: page table, replacer,
 *       free list, and every pin and every pin count reaching 0. Every fetch takes it briefly,
 *       because a fetch needs the page table and changes the LRU order. Closing a guard takes
 *       it only for the last unpin of a page; otherwise the atomic pin count is lowered
 *       without it. In this version it is also held during the disk I/O of a miss.</li>
 *   <li>A <b>page latch</b> (one per frame, {@link LatchMode READ or WRITE}) protects a page's
 *       bytes. Readers share it; a writer has it alone. It's what stops two threads from, say,
 *       inserting into the same slotted page at the same moment and overwriting each other.</li>
 * </ul>
 * One ordering rule keeps them from deadlocking: <b>never wait for a page latch while holding
 * the pool latch.</b> A fetch pins under the pool latch, releases it, and only then waits for
 * the page latch. The other order (holding a page latch, then taking the pool latch) is
 * allowed and happens all the time: a thread holding one page fetches another.
 *
 * <p>The traps the pool latch still rules out by construction:
 * <ul>
 *   <li>A page is never loaded into two frames at once (two threads missing the same page
 *       one after the other: the second one finds it in the table).</li>
 *   <li>A page can't be evicted between being found and being pinned.</li>
 *   <li>The DiskManager's rule "no two I/Os on the same page at once" holds: loading and
 *       evicting happen only for unpinned frames, under the pool latch, and flushing a pinned
 *       page happens under its page latch and the flush lock.</li>
 * </ul>
 * The remaining price: one slow disk read blocks every other fetch. A later version will
 * release the pool latch during I/O and mark the frame "I/O in progress".
 *
 * <p>Write-back doesn't fsync; durability stays with the caller ({@link DiskManager#sync}),
 * and later with the WAL, which will also have to be flushed before a dirty page is written.
 */
public final class BufferPool implements AutoCloseable {

    private final DiskManager disk;
    private final int frameCount;

    // Shared: frames are used by many threads.
    private final Arena arena = Arena.ofShared();
    private final Frame[] frames;

    // Guarded by `poolLatch`.
    private final LongIntHashMap pageTable;
    private final LruReplacer replacer;
    private final int[] freeFrames;
    private int freeCount;
    private boolean closed;

    /**
     * {@code synchronized}, not ReentrantLock, for the same reason as in DiskManager: a
     * contended monitor allocates nothing on the Java heap.
     */
    private final Object poolLatch = new Object();

    /**
     * Lets only one thread at a time write pinned pages back. Flushing takes a page's READ
     * latch, so that readers can keep reading during the write; this lock is what keeps two
     * flushers from writing the same page at once (the DiskManager forbids that). Flushing is
     * a cold path (checkpoints, shutdown), so serialising it costs nothing that matters.
     *
     * <p>Lock order: flushLock, then a page latch, then the pool latch.
     */
    private final Object flushLock = new Object();

    public BufferPool(DiskManager disk, int frameCount) {
        this.disk = Objects.requireNonNull(disk, "disk");
        if (frameCount < 1) {
            throw new IllegalArgumentException("frameCount must be at least 1: " + frameCount);
        }
        this.frameCount = frameCount;

        // Aligned to 4096: Java doesn't need it, but unbuffered I/O (O_DIRECT) in a future
        // native backend does, and aligning now costs nothing.
        MemorySegment memory = arena.allocate((long) frameCount * PAGE_SIZE, 4096);
        this.frames = new Frame[frameCount];
        for (int i = 0; i < frameCount; i++) {
            frames[i] = new Frame(i, memory.asSlice((long) i * PAGE_SIZE, PAGE_SIZE));
        }

        this.pageTable = new LongIntHashMap(frameCount);
        this.replacer = new LruReplacer(frameCount);
        // A stack; filled backwards so frame 0 is handed out first.
        this.freeFrames = new int[frameCount];
        for (int i = 0; i < frameCount; i++) {
            freeFrames[i] = frameCount - 1 - i;
        }
        this.freeCount = frameCount;
    }

    public int frameCount() {
        return frameCount;
    }

    // ---------------------------------------------------------------- page access (hot)

    /**
     * Pins page {@code pageNo} of {@code fileId}, reading it from disk if it isn't in memory,
     * latches it in {@code mode}, and hands it to {@code guard}. Close the guard to release it.
     *
     * <p>Waits while another thread holds a conflicting latch on the page (any WRITE, or READ
     * when asking for WRITE). While waiting, the page stays pinned, and no pool latch is held.
     *
     * @throws BufferPoolFullException if every frame is pinned
     * @throws IllegalStateException if the guard still holds a page, or this thread already
     *         holds this page with a WRITE latch (waiting would deadlock)
     * @throws IndexOutOfBoundsException if the page doesn't exist
     * @throws IllegalArgumentException if the file isn't open in the DiskManager
     */
    public void fetchPage(int fileId, int pageNo, LatchMode mode, PageGuard guard) {
        Objects.requireNonNull(mode, "mode");
        guard.checkEmpty();
        Frame frame = pin(fileId, pageNo);
        guard.attach(this, frame, mode, latch(frame, mode));
    }

    /**
     * Appends a new, zero-filled page to {@code fileId}, pins it, and hands it to {@code guard}
     * with a WRITE latch, because a new page is there to be filled. The page number is available
     * as {@link PageGuard#pageNo()}. No disk read: we know the page is all zeros.
     *
     * @throws BufferPoolFullException if every frame is pinned (no page is added to the file then)
     */
    public void newPage(int fileId, PageGuard guard) {
        guard.checkEmpty();
        Frame frame = pinNew(fileId);
        guard.attach(this, frame, LatchMode.WRITE, latch(frame, LatchMode.WRITE));
    }

    /** Caller holds the pin and no pool latch. */
    private long latch(Frame frame, LatchMode mode) {
        if (frame.writer == Thread.currentThread()) {
            unpin(frame, false);
            throw new IllegalStateException("this thread already holds page " + PageKeys.pageNo(frame.pageKey())
                    + " of file " + PageKeys.fileId(frame.pageKey())
                    + " with a WRITE latch; the latch is not reentrant, so waiting would deadlock");
        }
        if (mode == LatchMode.WRITE) {
            long stamp = frame.latch.writeLock();
            frame.writer = Thread.currentThread();
            return stamp;
        }
        return frame.latch.readLock();
    }

    // ---------------------------------------------------------------- write-back (no fsync)

    /**
     * Writes the page to disk if it is in the pool and dirty. Changes count once their guard is
     * closed. If a thread holds the page with WRITE, this waits until that guard is closed, so a
     * half-done change is never written.
     *
     * <p>The calling thread must hold no guards: flushing waits for page latches, and a thread
     * that holds one could end up waiting for itself or for a thread that waits for it.
     */
    public void flushPage(int fileId, int pageNo) {
        Frame frame;
        synchronized (poolLatch) {
            ensureOpen();
            int id = pageTable.get(PageKeys.pack(fileId, pageNo));
            if (id == LongIntHashMap.MISSING || !frames[id].isDirty()) {
                return;
            }
            frame = frames[id];
            pinResident(frame);
        }
        flushPinned(frame);
    }

    /** Writes every dirty page of one file. Call it before closing that file in the DiskManager. */
    public void flushFile(int fileId) {
        flushDirty(false, fileId);
    }

    public void flushAll() {
        flushDirty(true, 0);
    }

    /**
     * Frees the pool's memory. Does <em>not</em> flush: call {@link #flushAll()} first if the
     * changes must reach the files. Idempotent.
     *
     * @throws IllegalStateException if pages are still pinned. Their guards would point into
     *         freed memory, and it usually means a guard was never closed.
     */
    @Override
    public void close() {
        synchronized (poolLatch) {
            if (closed) {
                return;
            }
            int pinned = 0;
            for (Frame frame : frames) {
                if (frame.pinCount() > 0) {
                    pinned++;
                }
            }
            if (pinned > 0) {
                throw new IllegalStateException("cannot close: " + pinned + " page(s) still pinned");
            }
            closed = true;
            arena.close();
        }
    }

    // ---------------------------------------------------------------- core (PageGuard and tests)

    Frame pin(int fileId, int pageNo) {
        long key = PageKeys.pack(fileId, pageNo);
        synchronized (poolLatch) {
            ensureOpen();
            int id = pageTable.get(key);
            if (id != LongIntHashMap.MISSING) {
                Frame frame = frames[id];
                pinResident(frame);
                return frame;
            }

            // Miss. Validate before evicting anything: a bad request must not cost another
            // page its frame. pageCount also throws if the file isn't open.
            Objects.checkIndex(pageNo, disk.pageCount(fileId));
            Frame frame = takeFrame();
            try {
                disk.readPage(fileId, pageNo, frame.buffer);
            } catch (RuntimeException e) {
                pushFree(frame);
                throw e;
            }
            install(frame, key);
            return frame;
        }
    }

    Frame pinNew(int fileId) {
        synchronized (poolLatch) {
            ensureOpen();
            // Take the frame first: if the pool is full, nothing is added to the file.
            Frame frame = takeFrame();
            int pageNo;
            try {
                pageNo = disk.allocatePage(fileId);
            } catch (RuntimeException e) {
                pushFree(frame);
                throw e;
            }
            frame.segment.fill((byte) 0);
            install(frame, PageKeys.pack(fileId, pageNo));
            return frame;
        }
    }

    /**
     * Takes the pool latch only for the last unpin. While other holders remain, nothing about
     * the frame's place in the pool changes, so the atomic decrement is all there is to do.
     *
     * <p>The last unpin then has a window: the count is already 0, but the frame isn't in the
     * replacer yet. Under the pool latch it has to check what happened in that window:
     * <ul>
     *   <li>a fetch pinned it again: the count is above 0, and it must stay out of the replacer;</li>
     *   <li>another unpin to 0 already added it: adding again only moves it to the MRU end;</li>
     *   <li>it was evicted, and its next page's load failed: it is empty and on the free list,
     *       and must not also go into the replacer.</li>
     * </ul>
     * Until then the frame is simply not evictable, which is safe, just briefly pessimistic.
     *
     * <p>The dirty flag is set before the decrement: once the count is 0 the frame may be
     * evicted, and the eviction must see the flag to write the page back.
     */
    void unpin(Frame frame, boolean dirty) {
        if (dirty) {
            frame.markDirty();
        }
        if (frame.unpin() > 0) {
            return;
        }
        synchronized (poolLatch) {
            if (frame.pinCount() == 0 && !frame.isEmpty()) {
                replacer.add(frame.id);
            }
        }
    }

    // ---------------------------------------------------------------- internals

    /**
     * Caller holds poolLatch; the frame holds a page.
     *
     * <p>Decides on the count {@code pin()} returns, not on one read before it: an unpin without
     * the latch can lower the count between a read and the increment. If the count went 0 -> 1,
     * the frame may be in the replacer (or its last unpin is still on the way there, in which
     * case {@code remove} does nothing and that unpin will see the count above 0).
     */
    private void pinResident(Frame frame) {
        if (frame.pin() == 1) {
            replacer.remove(frame.id);
        }
    }

    /**
     * Returns an empty frame, unpinned and in no list: a free one if there is one, otherwise
     * the least recently used page's frame after evicting that page. Caller holds poolLatch.
     *
     * <p>Writing the victim back needs no page latch: it has pin count 0, and guards release
     * their latch before they unpin, so nobody can be holding it.
     */
    private Frame takeFrame() {
        if (freeCount > 0) {
            return frames[freeFrames[--freeCount]];
        }
        int id = replacer.pollVictim();
        if (id == -1) {
            throw new BufferPoolFullException(frameCount);
        }
        Frame victim = frames[id];
        try {
            writeBack(victim);
        } catch (RuntimeException e) {
            // Couldn't save its changes, so it can't be evicted. Put it back.
            replacer.add(id);
            throw e;
        }
        pageTable.remove(victim.pageKey());
        victim.reset();
        return victim;
    }

    /** Caller holds poolLatch. */
    private void install(Frame frame, long key) {
        frame.assign(key);
        frame.pin();
        pageTable.put(key, frame.id);
    }

    /** Writes a frame's page to disk if dirty and marks it clean. Caller holds poolLatch. */
    private void writeBack(Frame frame) {
        if (frame.isDirty()) {
            long key = frame.pageKey();
            disk.writePage(PageKeys.fileId(key), PageKeys.pageNo(key), frame.buffer);
            frame.markClean();
        }
    }

    /** Flushes, one at a time, every dirty page (of {@code fileId} unless {@code allFiles}). */
    private void flushDirty(boolean allFiles, int fileId) {
        for (Frame frame : frames) {
            synchronized (poolLatch) {
                ensureOpen();
                if (frame.isEmpty() || !frame.isDirty()
                        || (!allFiles && PageKeys.fileId(frame.pageKey()) != fileId)) {
                    continue;
                }
                pinResident(frame);
            }
            flushPinned(frame);
        }
    }

    /**
     * Writes a page that the caller has pinned for this purpose, then unpins it. Caller holds no
     * pool latch.
     *
     * <p>The READ latch makes writers wait, so the page is written in a consistent state, while
     * readers can carry on. The pin keeps the page from being evicted meanwhile, which also
     * means no other code path does I/O on this frame; the flushLock covers other flushers.
     */
    private void flushPinned(Frame frame) {
        if (frame.writer == Thread.currentThread()) {
            unpin(frame, false);
            throw new IllegalStateException("cannot flush a page this thread holds with a WRITE latch; close its guard first");
        }
        synchronized (flushLock) {
            long stamp = frame.latch.readLock();
            try {
                boolean dirty;
                synchronized (poolLatch) {
                    // Another flusher may have written it while we waited.
                    dirty = frame.isDirty();
                }
                if (dirty) {
                    long key = frame.pageKey(); // fixed while pinned
                    disk.writePage(PageKeys.fileId(key), PageKeys.pageNo(key), frame.buffer);
                    synchronized (poolLatch) {
                        // Still under the READ latch, so no change can have slipped in since the
                        // write. A writer that finished just before we got the latch may still
                        // mark the page dirty when it unpins; that only costs one extra write.
                        frame.markClean();
                    }
                }
            } finally {
                frame.latch.unlockRead(stamp);
                unpin(frame, false);
            }
        }
    }

    /** Caller holds poolLatch. */
    private void pushFree(Frame frame) {
        frame.reset();
        freeFrames[freeCount++] = frame.id;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("BufferPool is closed");
        }
    }

    // ---------------------------------------------------------------- test helpers (cold)

    /** The page's pin count, or -1 if it isn't in the pool. */
    int pinCount(int fileId, int pageNo) {
        synchronized (poolLatch) {
            int id = pageTable.get(PageKeys.pack(fileId, pageNo));
            return id == LongIntHashMap.MISSING ? -1 : frames[id].pinCount();
        }
    }

    boolean isDirty(int fileId, int pageNo) {
        synchronized (poolLatch) {
            int id = pageTable.get(PageKeys.pack(fileId, pageNo));
            return id != LongIntHashMap.MISSING && frames[id].isDirty();
        }
    }

    int residentPageCount() {
        synchronized (poolLatch) {
            return pageTable.size();
        }
    }

    /**
     * Checks that the bookkeeping agrees with itself; throws if not. Every frame must be in
     * exactly one place: the free list (empty), the replacer (unpinned page), or pinned (in
     * neither). The page table must point at exactly the frames that hold pages. And an
     * unpinned frame must not be latched, because eviction relies on that.
     *
     * <p>Only call it while no guard is being closed: a last unpin lowers the count before it
     * takes the pool latch, and in that moment "unpinned but not in the replacer" is expected.
     */
    void checkInvariants() {
        synchronized (poolLatch) {
            boolean[] free = new boolean[frameCount];
            for (int i = 0; i < freeCount; i++) {
                check(!free[freeFrames[i]], "frame " + freeFrames[i] + " is on the free list twice");
                free[freeFrames[i]] = true;
            }
            int resident = 0;
            for (Frame frame : frames) {
                if (frame.pinCount() == 0) {
                    check(!frame.latch.isReadLocked() && !frame.latch.isWriteLocked() && frame.writer == null,
                            "unpinned frame " + frame.id + " is latched");
                }
                if (frame.isEmpty()) {
                    check(free[frame.id], "empty frame " + frame.id + " is not on the free list");
                    check(frame.pinCount() == 0 && !frame.isDirty(), "empty frame " + frame.id + " has state");
                    check(!replacer.contains(frame.id), "empty frame " + frame.id + " is in the replacer");
                } else {
                    resident++;
                    check(!free[frame.id], "frame " + frame.id + " holds a page but is on the free list");
                    check(pageTable.get(frame.pageKey()) == frame.id, "page table doesn't point at frame " + frame.id);
                    check(replacer.contains(frame.id) == (frame.pinCount() == 0),
                            "frame " + frame.id + ": in replacer must mean unpinned");
                }
            }
            check(pageTable.size() == resident, "page table has " + pageTable.size() + " entries, " + resident + " frames hold pages");
            check(replacer.size() + freeCount <= frameCount, "replacer and free list overlap");
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException("BufferPool invariant broken: " + message);
        }
    }
}
