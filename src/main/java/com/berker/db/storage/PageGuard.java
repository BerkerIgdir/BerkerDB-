package com.berker.db.storage;

import java.lang.foreign.MemorySegment;

/**
 * A pinned page. While the guard holds it, the page stays in memory; {@link #close()} unpins
 * it.
 *
 * <p><b>Reused, not created per fetch.</b> A guard is created once (for example when a scan
 * starts) and passed to every {@link BufferPool#fetchPage} call, which fills in its fields.
 * Returning a new guard from each fetch would allocate once per page access, which is exactly
 * what the hot path must not do. In C++ (BusTub) a guard returned by value lives on the stack
 * and costs nothing; in Java it would live on the heap.
 *
 * <pre>{@code
 * PageGuard guard = new PageGuard();          // once
 * for (...) {
 *     pool.fetchPage(fileId, pageNo, guard);  // no allocation
 *     try (guard) {                           // Java 9+: try on an existing variable
 *         ... guard.segment() ...
 *     }                                       // close() unpins
 * }
 * }</pre>
 *
 * <p>Rules that come with reuse:
 * <ul>
 *   <li>One guard holds at most one page. Fetching into a guard that still holds a page throws,
 *       rather than silently unpinning the old one. Code that needs two pages at once uses two
 *       guards.</li>
 *   <li>A guard belongs to one thread.</li>
 *   <li>Don't keep the segment after {@code close()}: by then the frame may hold a different
 *       page. The guard itself refuses {@link #segment()} once closed, but a reference you
 *       copied out can't be checked.</li>
 * </ul>
 */
public final class PageGuard implements AutoCloseable {

    private BufferPool pool;
    private Frame frame;      // null = holds nothing
    private boolean dirty;

    public PageGuard() {
    }

    /** The page's 8 KB of memory. */
    public MemorySegment segment() {
        return held().segment;
    }

    public int fileId() {
        return PageKeys.fileId(held().pageKey());
    }

    public int pageNo() {
        return PageKeys.pageNo(held().pageKey());
    }

    /**
     * Records that this page was changed, so the pool writes it back before reusing its frame.
     * Only sets a field here; the pool learns about it on {@link #close()}, so modifying a page
     * never touches shared state.
     */
    public void markDirty() {
        held();
        dirty = true;
    }

    public boolean isHeld() {
        return frame != null;
    }

    /** Unpins the page. Does nothing if the guard holds no page, so closing twice is safe. */
    @Override
    public void close() {
        Frame f = frame;
        if (f == null) {
            return;
        }
        boolean wasDirty = dirty;
        // Clear first: even if unpin throws, this guard must not look like it still holds the page.
        frame = null;
        dirty = false;
        pool.unpin(f, wasDirty);
    }

    /** Called by the pool before pinning, so a held guard is rejected before anything is pinned. */
    void checkEmpty() {
        if (frame != null) {
            throw new IllegalStateException("guard still holds page " + pageNo()
                    + " of file " + fileId() + "; close it before fetching another page");
        }
    }

    /** Called by the pool once the frame is pinned. */
    void attach(BufferPool pool, Frame frame) {
        this.pool = pool;
        this.frame = frame;
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
