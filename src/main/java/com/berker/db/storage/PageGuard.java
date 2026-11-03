package com.berker.db.storage;

import java.lang.foreign.MemorySegment;

/**
 * A page that is pinned <em>and</em> latched. While the guard holds it:
 * <ul>
 *   <li>the page stays in its frame (the pin), and</li>
 *   <li>its bytes are protected by the page latch: with {@link LatchMode#READ} nobody can
 *       change them, with {@link LatchMode#WRITE} nobody else can even read them.</li>
 * </ul>
 * {@link #close()} releases the latch, then the pin.
 *
 * <p>The latch only protects one physical read or change. It does not keep two transactions
 * from overwriting each other's rows over time; that is concurrency control's job, above the
 * BufferPool. Close a guard as soon as the read or change is done.
 *
 * <p><b>Reused, not created per fetch.</b> A guard is created once (for example when a scan
 * starts) and passed to every fetch, which fills in its fields. Returning a new guard from each
 * fetch would allocate once per page access, which is exactly what the hot path must not do. In
 * C++ (BusTub) a guard returned by value lives on the stack and costs nothing; in Java it would
 * live on the heap.
 *
 * <pre>{@code
 * PageGuard guard = new PageGuard();                           // once
 * for (...) {
 *     pool.fetchPage(fileId, pageNo, LatchMode.READ, guard);   // no allocation
 *     try (guard) {                                            // Java 9+: try on an existing variable
 *         ... guard.segment() ...
 *     }                                                        // close(): unlatch, unpin
 * }
 * }</pre>
 *
 * <p>Rules:
 * <ul>
 *   <li>One guard holds at most one page. Fetching into a guard that still holds a page throws,
 *       rather than silently releasing the old one. Code that needs two pages at once uses two
 *       guards.</li>
 *   <li>A guard belongs to one thread, and must be closed by that thread.</li>
 *   <li>The latch is not reentrant. A thread must not fetch a page it already holds with
 *       {@code WRITE} (that is detected and throws), nor ask for {@code WRITE} on a page it
 *       already holds with {@code READ} (that is <em>not</em> detected: it waits for itself
 *       forever). Holding the same page twice with {@code READ} is fine.</li>
 *   <li>Don't keep the segment after {@code close()}: by then another thread may be changing
 *       the page, or the frame may hold a different page. The guard itself refuses
 *       {@link #segment()} once closed, but a reference you copied out can't be checked.</li>
 * </ul>
 */
public final class PageGuard implements AutoCloseable {

    private BufferPool pool;
    private Frame frame;       // null = holds nothing
    private LatchMode mode;
    private long stamp;        // StampedLock's ticket for unlocking
    private boolean dirty;

    public PageGuard() {
    }

    /**
     * The page's 8 KB of memory. Read-only for a {@code READ} guard: writing through it throws
     * {@link IllegalArgumentException}.
     */
    public MemorySegment segment() {
        Frame f = held();
        return mode == LatchMode.WRITE ? f.segment : f.readOnlySegment;
    }

    public LatchMode mode() {
        held();
        return mode;
    }

    public int fileId() {
        return PageKeys.fileId(held().pageKey());
    }

    public int pageNo() {
        return PageKeys.pageNo(held().pageKey());
    }

    /**
     * Records that this page was changed, so the pool writes it back before reusing its frame.
     * Only allowed with {@code WRITE}. Only sets a field here; the pool learns about it on
     * {@link #close()}, so changing a page never touches the pool's shared state.
     */
    public void markDirty() {
        held();
        if (mode != LatchMode.WRITE) {
            throw new IllegalStateException("markDirty needs a WRITE guard; this one is READ");
        }
        dirty = true;
    }

    public boolean isHeld() {
        return frame != null;
    }

    /**
     * Releases the latch, then unpins. Does nothing if the guard holds no page, so closing twice
     * is safe.
     *
     * <p>The latch goes first so that an unpinned frame never has a latch holder. Eviction
     * relies on that: it rewrites a frame with pin count 0 without taking its latch.
     */
    @Override
    public void close() {
        Frame f = frame;
        if (f == null) {
            return;
        }
        LatchMode m = mode;
        long s = stamp;
        boolean wasDirty = dirty;
        // Clear first: even if something below throws, this guard must not look like it still
        // holds the page.
        frame = null;
        mode = null;
        stamp = 0;
        dirty = false;
        if (m == LatchMode.WRITE) {
            f.writer = null;
            f.latch.unlockWrite(s);
        } else {
            f.latch.unlockRead(s);
        }
        pool.unpin(f, wasDirty);
    }

    /** Called by the pool before pinning, so a held guard is rejected before anything is pinned. */
    void checkEmpty() {
        if (frame != null) {
            throw new IllegalStateException("guard still holds page " + pageNo()
                    + " of file " + fileId() + "; close it before fetching another page");
        }
    }

    /** Called by the pool once the frame is pinned and latched. */
    void attach(BufferPool pool, Frame frame, LatchMode mode, long stamp) {
        this.pool = pool;
        this.frame = frame;
        this.mode = mode;
        this.stamp = stamp;
        this.dirty = false;
    }

    private Frame held() {
        Frame f = frame;
        if (f == null) {
            throw new IllegalStateException("guard holds no page");
        }
        return f;
    }
}
