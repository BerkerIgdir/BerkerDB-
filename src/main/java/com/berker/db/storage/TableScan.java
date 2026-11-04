package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * A full scan of a {@link TableHeap}: every live row of every page, in page and slot order.
 *
 * <pre>{@code
 * try (TableScan scan = new TableScan(heap, layout)) {
 *     while (scan.next()) {
 *         long id = scan.tuple().getLong(0);
 *     }
 * }
 * }</pre>
 *
 * <p>The scan copies each page into a private 8 KB buffer, holding the latch only for the copy,
 * then walks the rows in that copy. So no latch is held while the caller works on a row, however
 * long that takes, and each page costs one fetch instead of one per row. The copy is about a
 * microsecond of memory move, small next to reading the page from disk.
 *
 * <p>{@link #tuple()} always returns the same view, valid until the next {@link #next}. The
 * scan's buffer lives in its own confined arena, so a scan belongs to one thread. Creating one
 * allocates; to go over a table again without allocating, {@link #rewind} it.
 */
public final class TableScan implements AutoCloseable {

    private final TableHeap heap;
    private final Arena arena = Arena.ofConfined();
    private final MemorySegment buffer = arena.allocate(PAGE_SIZE, Long.BYTES);
    private final SlottedPage page = new SlottedPage();
    private final PageGuard guard = new PageGuard();
    private final TupleView tuple;
    private int pageNo;
    private int slot;
    private int slotCount;

    public TableScan(TableHeap heap, TupleLayout layout) {
        this.heap = heap;
        this.tuple = new TupleView(layout);
        page.wrap(buffer);
        rewind();
    }

    /** Goes back to before the first row. */
    public void rewind() {
        pageNo = -1;
        slot = -1;
        slotCount = 0;
    }

    /** Moves to the next row. Returns false if there are no more. */
    public boolean next() {
        while (true) {
            while (slot + 1 < slotCount) {
                slot++;
                if (page.isLive(slot)) {
                    tuple.wrap(buffer, page.tupleOffset(slot), page.tupleLength(slot));
                    return true;
                }
            }
            if (pageNo + 1 >= heap.pageCount()) {
                return false;
            }
            pageNo++;
            heap.copyPage(pageNo, guard, buffer);
            slot = -1;
            slotCount = page.slotCount();
        }
    }

    /** The current row, valid until the next {@link #next}. */
    public TupleView tuple() {
        return tuple;
    }

    /** The current row's record ID. */
    public long rid() {
        return Rids.pack(pageNo, slot);
    }

    @Override
    public void close() {
        arena.close();
    }
}
