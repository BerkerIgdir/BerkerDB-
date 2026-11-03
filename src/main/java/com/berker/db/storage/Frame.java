package com.berker.db.storage;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * One buffer frame: an 8 KB slot of the BufferPool's memory plus what the pool knows about
 * the page in it.
 *
 * <p>All frames are created once when the pool starts and live as long as the pool. Fetching
 * a different page into a frame only overwrites its fields, so frames cost nothing on the hot
 * path. The low-GC rule is about not <em>allocating</em> per page access, not about avoiding
 * objects altogether.
 *
 * <p>Package-private. Code outside the package only ever sees a {@link PageGuard}, so it can't
 * change a pin count behind the pool's back. The pool needs to know when a count reaches zero,
 * because that's when the frame becomes a candidate for eviction.
 *
 * <p>Most metadata is changed only under the pool latch. The exceptions are {@link #unpin()}
 * and {@link #markDirty()}: a closing guard calls them without the pool latch, so the pin count
 * is atomic and the dirty flag volatile.
 */
final class Frame {

    static final long EMPTY = -1L;

    /** Position in the pool; also this frame's slot in the page table and LruReplacer. */
    final int id;
    /** The frame's 8 KB of the pool's big segment. WRITE guards hand this out. */
    final MemorySegment segment;
    /**
     * The same memory, read-only. READ guards hand this out, so a write through a READ guard
     * fails immediately instead of racing with other readers. Created once here because
     * {@code asReadOnly()} allocates a new segment object on every call.
     */
    final MemorySegment readOnlySegment;
    /**
     * A view of the same memory for DiskManager, which needs a ByteBuffer. Created once here
     * because {@code asByteBuffer()} allocates a new wrapper on every call.
     */
    final ByteBuffer buffer;

    /**
     * The page latch: shared for READ guards, exclusive for WRITE guards.
     *
     * <p>Why StampedLock and not ReentrantReadWriteLock: ReentrantReadWriteLock counts read
     * holds per thread, and every additional reader thread allocates a hold counter when it
     * takes the lock, so plain concurrent reads would allocate. StampedLock keeps no per-thread
     * state: taking it is one CAS, and it allocates only when a thread actually has to wait
     * (a queue node). The price is that it is <em>not reentrant</em>, see {@link #writer}.
     *
     * <p>Not guarded by the pool latch; that's the point. Threads wait for page latches only
     * while holding no pool latch, otherwise a page-latch holder that needs the pool latch would
     * deadlock with them.
     */
    final StampedLock latch = new StampedLock();

    /**
     * The thread holding the WRITE latch, or null. Only used to turn a certain deadlock into an
     * exception: StampedLock doesn't know its owner, so a thread asking again for a page it
     * holds with WRITE would wait for itself forever.
     *
     * <p>A plain field is enough. A thread only ever compares it with itself, and it only ever
     * holds the current thread's own value if that thread wrote it, so a stale read by another
     * thread can never look like "it's me".
     */
    Thread writer;

    /** Guarded by the pool latch. Fixed while the frame is pinned. */
    private long pageKey = EMPTY;

    /**
     * Raised only under the pool latch, because a pin goes with a page-table lookup and a
     * replacer update. Lowered <em>without</em> it, so a guard closing on a page others still
     * hold doesn't touch the pool latch at all; only the last unpin (count reaching 0) takes it,
     * to make the frame evictable.
     *
     * <p>An {@code AtomicInteger} created once here, so pinning allocates nothing. A
     * {@code VarHandle} on a plain {@code int} field would save the extra object per frame, at the
     * cost of harder-to-read code; with a few thousand frames that saving doesn't matter.
     */
    private final AtomicInteger pinCount = new AtomicInteger();

    /**
     * Volatile because a closing WRITE guard sets it without the pool latch. It is always set
     * <em>before</em> that guard's unpin, while the pin still keeps the frame from being evicted;
     * the atomic decrement then publishes it to whoever evicts the frame later.
     */
    private volatile boolean dirty;

    Frame(int id, MemorySegment segment) {
        this.id = id;
        this.segment = segment;
        this.readOnlySegment = segment.asReadOnly();
        this.buffer = segment.asByteBuffer();
    }

    long pageKey() {
        return pageKey;
    }

    int pinCount() {
        return pinCount.get();
    }

    boolean isDirty() {
        return dirty;
    }

    boolean isEmpty() {
        return pageKey == EMPTY;
    }

    /** A new page now lives in this frame: clean and unpinned. Caller holds the pool latch. */
    void assign(long pageKey) {
        this.pageKey = pageKey;
        this.pinCount.set(0);
        this.dirty = false;
    }

    /** Caller holds the pool latch. @return the pin count after pinning */
    int pin() {
        return pinCount.incrementAndGet();
    }

    /**
     * Safe without the pool latch.
     *
     * <p>A compare-and-set loop rather than {@code decrementAndGet()}, so an unpin too many is
     * refused before the count goes negative, instead of noticed after. It allocates nothing;
     * {@code updateAndGet(v -> ...)} would read nicer but is a lambda in the hot path.
     *
     * @return the pin count after unpinning
     */
    int unpin() {
        while (true) {
            int count = pinCount.get();
            if (count == 0) {
                throw new IllegalStateException("frame " + id + " is not pinned");
            }
            if (pinCount.compareAndSet(count, count - 1)) {
                return count - 1;
            }
        }
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
        pinCount.set(0);
        dirty = false;
    }
}
