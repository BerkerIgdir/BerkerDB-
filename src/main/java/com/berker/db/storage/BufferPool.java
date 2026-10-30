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
 * per frame holds the page's metadata plus a slice and a ByteBuffer view of its memory. All of
 * it is created in the constructor; afterwards fetching, evicting and flushing allocate
 * nothing.
 *
 * <p><b>Finding pages.</b> {@link LongIntHashMap} maps a page key ({@link PageKeys}) to a frame
 * number, and {@link LruReplacer} picks the least recently used unpinned frame to evict.
 * Frames that never held a page, or lost theirs to a failed read, sit on a free list and are
 * used first.
 *
 * <p><b>Threads (version 1).</b> Every method may be called from any thread. One global latch
 * protects the page table, the replacer, the free list and all frame metadata, and it is held
 * <em>during disk I/O too</em>. That rules out the classic traps by construction:
 * <ul>
 *   <li>A page is never loaded into two frames at once (two threads missing the same page
 *       one after the other: the second one finds it in the table).</li>
 *   <li>A page can't be evicted between being found and being pinned.</li>
 *   <li>The DiskManager's rule "no two I/Os on the same page at once" holds automatically.</li>
 * </ul>
 * The price: one slow disk read blocks every thread, even those whose pages are in memory. A
 * later version will release the latch during I/O and mark the frame "I/O in progress".
 *
 * <p>The latch protects the pool's bookkeeping, not page <em>contents</em>. Two threads holding
 * the same page can both read and write its bytes; coordinating that is the job of page
 * latches, which come later. A pinned frame's page key and memory never change, so a guard
 * can read them without the latch.
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

    // Guarded by `latch`.
    private final LongIntHashMap pageTable;
    private final LruReplacer replacer;
    private final int[] freeFrames;
    private int freeCount;
    private boolean closed;

    /**
     * {@code synchronized}, not ReentrantLock, for the same reason as in DiskManager: a
     * contended monitor allocates nothing on the Java heap.
     */
    private final Object latch = new Object();

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
     * and hands it to {@code guard}. Close the guard to unpin.
     *
     * @throws BufferPoolFullException if every frame is pinned
     * @throws IllegalStateException if the guard still holds a page
     * @throws IndexOutOfBoundsException if the page doesn't exist
     * @throws IllegalArgumentException if the file isn't open in the DiskManager
     */
    public void fetchPage(int fileId, int pageNo, PageGuard guard) {
        guard.checkEmpty();
        guard.attach(this, pin(fileId, pageNo));
    }

    /**
     * Appends a new, zero-filled page to {@code fileId}, pins it and hands it to {@code guard}.
     * The page number is available as {@link PageGuard#pageNo()}. No disk read: we know the
     * page is all zeros.
     *
     * @throws BufferPoolFullException if every frame is pinned (no page is added to the file then)
     */
    public void newPage(int fileId, PageGuard guard) {
        guard.checkEmpty();
        guard.attach(this, pinNew(fileId));
    }

    // ---------------------------------------------------------------- write-back (no fsync)

    /** Writes the page to disk if it is in the pool and dirty. */
    public void flushPage(int fileId, int pageNo) {
        synchronized (latch) {
            ensureOpen();
            int id = pageTable.get(PageKeys.pack(fileId, pageNo));
            if (id != LongIntHashMap.MISSING) {
                writeBackIfDirty(frames[id]);
            }
        }
    }

    /** Writes every dirty page of one file. Call it before closing that file in the DiskManager. */
    public void flushFile(int fileId) {
        synchronized (latch) {
            ensureOpen();
            for (Frame frame : frames) {
                if (!frame.isEmpty() && PageKeys.fileId(frame.pageKey()) == fileId) {
                    writeBackIfDirty(frame);
                }
            }
        }
    }

    public void flushAll() {
        synchronized (latch) {
            ensureOpen();
            for (Frame frame : frames) {
                if (!frame.isEmpty()) {
                    writeBackIfDirty(frame);
                }
            }
        }
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
        synchronized (latch) {
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
        synchronized (latch) {
            ensureOpen();
            int id = pageTable.get(key);
            if (id != LongIntHashMap.MISSING) {
                Frame frame = frames[id];
                if (frame.pinCount() == 0) {
                    replacer.remove(id);
                }
                frame.pin();
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
        synchronized (latch) {
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

    void unpin(Frame frame, boolean dirty) {
        synchronized (latch) {
            if (dirty) {
                frame.markDirty();
            }
            if (frame.unpin() == 0) {
                replacer.add(frame.id);
            }
        }
    }

    // ---------------------------------------------------------------- internals (caller holds latch)

    /**
     * Returns an empty frame, unpinned and in no list: a free one if there is one, otherwise
     * the least recently used page's frame after evicting that page.
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
            writeBackIfDirty(victim);
        } catch (RuntimeException e) {
            // Couldn't save its changes, so it can't be evicted. Put it back.
            replacer.add(id);
            throw e;
        }
        pageTable.remove(victim.pageKey());
        victim.reset();
        return victim;
    }

    private void install(Frame frame, long key) {
        frame.assign(key);
        frame.pin();
        pageTable.put(key, frame.id);
    }

    private void writeBackIfDirty(Frame frame) {
        if (frame.isDirty()) {
            long key = frame.pageKey();
            disk.writePage(PageKeys.fileId(key), PageKeys.pageNo(key), frame.buffer);
            frame.markClean();
        }
    }

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
        synchronized (latch) {
            int id = pageTable.get(PageKeys.pack(fileId, pageNo));
            return id == LongIntHashMap.MISSING ? -1 : frames[id].pinCount();
        }
    }

    boolean isDirty(int fileId, int pageNo) {
        synchronized (latch) {
            int id = pageTable.get(PageKeys.pack(fileId, pageNo));
            return id != LongIntHashMap.MISSING && frames[id].isDirty();
        }
    }

    int residentPageCount() {
        synchronized (latch) {
            return pageTable.size();
        }
    }

    /**
     * Checks that the bookkeeping agrees with itself; throws if not. Every frame must be in
     * exactly one place: the free list (empty), the replacer (unpinned page), or pinned (in
     * neither). And the page table must point at exactly the frames that hold pages.
     */
    void checkInvariants() {
        synchronized (latch) {
            boolean[] free = new boolean[frameCount];
            for (int i = 0; i < freeCount; i++) {
                check(!free[freeFrames[i]], "frame " + freeFrames[i] + " is on the free list twice");
                free[freeFrames[i]] = true;
            }
            int resident = 0;
            for (Frame frame : frames) {
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
