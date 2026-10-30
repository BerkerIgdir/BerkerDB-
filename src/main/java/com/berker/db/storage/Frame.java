package com.berker.db.storage;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * One buffer frame: an 8 KB slot of the BufferPool's memory plus what the pool knows about
 * the page in it.
 *
 * <p>All frames are created once when the pool starts and live as long as the pool. Fetching
 * a different page into a frame only overwrites its fields, so frames cost nothing on the hot
 * path. The low-GC rule is about not <em>allocating</em> per page access, not about avoiding
 * objects altogether.
 *
 * <p>Package-private, and its mutators are only called by the BufferPool while it holds its
 * latch. Code outside the package only ever sees a {@link PageGuard}, so it can't change a pin
 * count behind the pool's back. The pool needs to know when a count reaches zero, because
 * that's when the frame becomes a candidate for eviction.
 */
final class Frame {

    static final long EMPTY = -1L;

    /** Position in the pool; also this frame's slot in the page table and LruReplacer. */
    final int id;
    /** The frame's 8 KB of the pool's big segment. Page code reads and writes through this. */
    final MemorySegment segment;
    /**
     * A view of the same memory for DiskManager, which needs a ByteBuffer. Created once here
     * because {@code asByteBuffer()} allocates a new wrapper on every call.
     */
    final ByteBuffer buffer;

    // Guarded by the pool's latch.
    private long pageKey = EMPTY;
    private int pinCount;
    private boolean dirty;

    Frame(int id, MemorySegment segment) {
        this.id = id;
        this.segment = segment;
        this.buffer = segment.asByteBuffer();
    }

    long pageKey() {
        return pageKey;
    }

    int pinCount() {
        return pinCount;
    }

    boolean isDirty() {
        return dirty;
    }

    boolean isEmpty() {
        return pageKey == EMPTY;
    }

    /** A new page now lives in this frame: clean and unpinned. */
    void assign(long pageKey) {
        this.pageKey = pageKey;
        this.pinCount = 0;
        this.dirty = false;
    }

    void pin() {
        pinCount++;
    }

    /** @return the pin count after unpinning */
    int unpin() {
        if (pinCount == 0) {
            throw new IllegalStateException("frame " + id + " is not pinned");
        }
        return --pinCount;
    }

    void markDirty() {
        dirty = true;
    }

    /** Called after the page was written back to disk. */
    void markClean() {
        dirty = false;
    }

    void reset() {
        pageKey = EMPTY;
        pinCount = 0;
        dirty = false;
    }
}
