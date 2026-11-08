package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * A heap file (Silberschatz §13.3): one table's rows in slotted pages, in no particular order. A
 * row goes on any page with room, and is found again by its record ID ({@link Rids}). It works in
 * bytes and knows nothing about columns: {@link TupleWriter} makes the bytes, {@link TupleView}
 * reads them.
 *
 * <p><b>Free-space map.</b> As in the book, finding a page with room doesn't mean visiting pages:
 * an array has one entry per page saying roughly how much room it has. An entry is a page's free
 * bytes divided by 32 and rounded down, so it never claims more room than there is. The book's
 * example uses 3 bits per page; one byte needs no bit packing and wastes less, because with 3
 * bits a page with under 1 KB free is never picked for small rows. A row needs its size divided
 * by 32 and rounded up, which is enough to guarantee it fits, unless the entry has gone stale.
 *
 * <p>The map is searched from the page last used, going round once, so a full table doesn't get
 * rescanned from page 0 for every row. The book's second-level map isn't built yet. It is also
 * not stored: it is rebuilt by reading every page when the heap is opened, which is slow for a
 * big table but can't be out of date. The book writes it to disk periodically and accepts staleness.
 *
 * <p><b>Thread-safe.</b> A page's bytes are protected by its page latch, and the map by a lock of
 * its own, held for a moment and never while waiting for anything. The order is page latch, then
 * map lock. A map entry can be stale for an instant: an insert may then find less room than the
 * entry promised. It corrects the entry and tries elsewhere, and since the corrected entry can't
 * promise that room again, the retry always moves on.
 *
 * <p><b>Rows are copied out, not exposed.</b> {@link #read} copies a row and {@link TableScan}
 * copies whole pages, so the caller never holds a latch while working on a row. Latches stay
 * short, and a caller that inserts during its own scan can't deadlock against itself (READ then
 * WRITE on a page, which the latch can't detect).
 *
 * <p>There are no transactions yet: a concurrent scan may or may not see a row being inserted,
 * updated or deleted.
 */
public final class TableHeap {

    private static final int FREE_UNIT = 32;

    private final BufferPool pool;
    private final int fileId;

    private final Object mapLock = new Object();
    private byte[] freeUnits;
    private int pageCount;
    private int searchFrom;

    /**
     * Opens the heap stored in {@code fileId}, which must be open in {@code disk}, and rebuilds
     * the free-space map. A page that was allocated but never written is formatted here.
     */
    public TableHeap(BufferPool pool, DiskManager disk, int fileId) {
        this.pool = pool;
        this.fileId = fileId;
        int pages = disk.pageCount(fileId);
        freeUnits = new byte[Math.max(pages, 16)];

        HeapContext context = new HeapContext();
        PageGuard guard = context.guard;
        for (int pageNo = 0; pageNo < pages; pageNo++) {
            pool.fetchPage(fileId, pageNo, LatchMode.WRITE, guard);
            try (guard) {
                SlottedPage page = context.page.wrap(guard.segment());
                if (!page.isFormatted()) {
                    page.init();
                    guard.markDirty();
                }
                freeUnits[pageNo] = (byte) (page.freeSpace() / FREE_UNIT);
            }
        }
        pageCount = pages;
    }

    public int fileId() {
        return fileId;
    }

    /** Pages in the heap. Rows are on pages {@code 0 .. pageCount-1}. */
    public int pageCount() {
        synchronized (mapLock) {
            return pageCount;
        }
    }

    /**
     * Stores a copy of {@code length} bytes from {@code src} as a new row, adding a page if none
     * has room.
     *
     * @return the row's record ID
     * @throws IllegalArgumentException if {@code length} is not between 1 and
     *         {@link SlottedPage#MAX_TUPLE_SIZE}
     */
    public long insert(HeapContext context, MemorySegment src, long srcOffset, int length) {
        if (length < 1 || length > SlottedPage.MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException(
                    "row length must be 1.." + SlottedPage.MAX_TUPLE_SIZE + ": " + length);
        }
        int units = (SlottedPage.align(length) + FREE_UNIT - 1) / FREE_UNIT;
        PageGuard guard = context.guard;
        while (true) {
            int pageNo = findPage(units);
            if (pageNo == -1) {
                return insertIntoNewPage(context, src, srcOffset, length);
            }
            pool.fetchPage(fileId, pageNo, LatchMode.WRITE, guard);
            try (guard) {
                SlottedPage page = context.page.wrap(guard.segment());
                int slot = page.insert(src, srcOffset, length);
                recordFreeSpace(pageNo, page.freeSpace());
                if (slot != -1) {
                    guard.markDirty();
                    return Rids.pack(pageNo, slot);
                }
            }
        }
    }

    private long insertIntoNewPage(HeapContext context, MemorySegment src, long srcOffset, int length) {
        PageGuard guard = context.guard;
        pool.newPage(fileId, guard);
        try (guard) {
            SlottedPage page = context.page.wrap(guard.segment());
            page.init();
            int slot = page.insert(src, srcOffset, length);
            guard.markDirty();
            recordFreeSpace(guard.pageNo(), page.freeSpace());
            return Rids.pack(guard.pageNo(), slot);
        }
    }

    /**
     * Copies a row into {@code dst}, which needs room for the whole row:
     * {@link SlottedPage#MAX_TUPLE_SIZE} bytes is always enough.
     *
     * @return the row's length, or -1 if there is no such row
     */
    public int read(HeapContext context, long rid, MemorySegment dst, long dstOffset) {
        int pageNo = Rids.pageNo(rid);
        int slot = Rids.slot(rid);
        if (pageNo < 0 || pageNo >= pageCount()) {
            return -1;
        }
        PageGuard guard = context.guard;
        pool.fetchPage(fileId, pageNo, LatchMode.READ, guard);
        try (guard) {
            SlottedPage page = context.page.wrap(guard.segment());
            if (!hasRow(page, slot)) {
                return -1;
            }
            int length = page.tupleLength(slot);
            MemorySegment.copy(guard.segment(), page.tupleOffset(slot), dst, dstOffset, length);
            return length;
        }
    }

    /** Deletes a row. Returns false if there is no such row. */
    public boolean delete(HeapContext context, long rid) {
        int pageNo = Rids.pageNo(rid);
        int slot = Rids.slot(rid);
        if (pageNo < 0 || pageNo >= pageCount()) {
            return false;
        }
        PageGuard guard = context.guard;
        pool.fetchPage(fileId, pageNo, LatchMode.WRITE, guard);
        try (guard) {
            SlottedPage page = context.page.wrap(guard.segment());
            if (!hasRow(page, slot)) {
                return false;
            }
            page.delete(slot);
            guard.markDirty();
            recordFreeSpace(pageNo, page.freeSpace());
            return true;
        }
    }

    /**
     * Replaces a row with {@code length} bytes from {@code src}.
     *
     * <p>If the new row fits on the row's page, it stays there, in the same slot, and the record
     * ID doesn't change. If it doesn't fit, the row moves: the new version is inserted like a
     * new row, then the old one is deleted, and the row gets a new record ID. The caller must
     * then change everything that keeps the old one, such as index entries. The other solution
     * is a forwarding pointer in the old slot, which keeps the record ID but can cost a second
     * page fetch on every read; it isn't built.
     *
     * <p>The move takes two steps, and no latch is held between them. Holding the old page while
     * fetching the new one would hold two page latches, which can deadlock against a thread that
     * takes them in the other order. Inserting before deleting means the row is never missing;
     * a concurrent scan may see it twice for a moment instead. There are no transactions yet.
     *
     * @return the row's record ID, which is a new one if the row moved, or -1 if there is no such row
     * @throws IllegalArgumentException if {@code length} is not between 1 and
     *         {@link SlottedPage#MAX_TUPLE_SIZE}
     */
    public long update(HeapContext context, long rid, MemorySegment src, long srcOffset, int length) {
        if (length < 1 || length > SlottedPage.MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException(
                    "row length must be 1.." + SlottedPage.MAX_TUPLE_SIZE + ": " + length);
        }
        int pageNo = Rids.pageNo(rid);
        int slot = Rids.slot(rid);
        if (pageNo < 0 || pageNo >= pageCount()) {
            return -1;
        }
        PageGuard guard = context.guard;
        pool.fetchPage(fileId, pageNo, LatchMode.WRITE, guard);
        try (guard) {
            SlottedPage page = context.page.wrap(guard.segment());
            if (!hasRow(page, slot)) {
                return -1;
            }
            if (page.update(slot, src, srcOffset, length)) {
                guard.markDirty();
                recordFreeSpace(pageNo, page.freeSpace());
                return rid;
            }
        }

        long newRid = insert(context, src, srcOffset, length);
        if (!delete(context, rid)) {
            // Another thread deleted the old row in between. The row is gone, so its new copy must go too.
            delete(context, newRid);
            return -1;
        }
        return newRid;
    }

    /** For {@link TableScan}: copies a whole page, holding its latch only for the copy. */
    void copyPage(int pageNo, PageGuard guard, MemorySegment dst) {
        pool.fetchPage(fileId, pageNo, LatchMode.READ, guard);
        try (guard) {
            MemorySegment.copy(guard.segment(), 0, dst, 0, PAGE_SIZE);
        }
    }

    private static boolean hasRow(SlottedPage page, int slot) {
        return slot >= 0 && slot < page.slotCount() && page.isLive(slot);
    }

    /** A page whose map entry is at least {@code units}, or -1. */
    private int findPage(int units) {
        synchronized (mapLock) {
            for (int i = 0; i < pageCount; i++) {
                int pageNo = searchFrom + i;
                if (pageNo >= pageCount) {
                    pageNo -= pageCount;
                }
                if ((freeUnits[pageNo] & 0xFF) >= units) {
                    searchFrom = pageNo;
                    return pageNo;
                }
            }
            return -1;
        }
    }

    private void recordFreeSpace(int pageNo, int freeSpace) {
        synchronized (mapLock) {
            if (pageNo >= freeUnits.length) {
                // Doubling allocates, but only each time the table doubles, never per row.
                freeUnits = Arrays.copyOf(freeUnits, Math.max(freeUnits.length * 2, pageNo + 1));
            }
            freeUnits[pageNo] = (byte) (freeSpace / FREE_UNIT);
            if (pageNo >= pageCount) {
                pageCount = pageNo + 1;
                searchFrom = pageNo;
            }
        }
    }
}
