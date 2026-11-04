package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * A flyweight over one 8 KB page laid out as a slotted page (Silberschatz §13.2.2): a header,
 * an array of slots growing forward, and tuples growing backward from the end of the page, with
 * one contiguous free gap in between.
 *
 * <pre>
 *  0        24                      freeStart          freeEnd                    8192
 *  [ header ][ slot 0 ][ slot 1 ]...[     free gap      ][ tuple 1 ][ tuple 0 ]
 *             offset+length (4 B)                         newest     oldest
 * </pre>
 *
 * <p><b>Header</b> (24 bytes, little-endian):
 * <pre>
 *  0   8  pageLSN    reserved for the WAL: the last log record that changed this page
 *  8   2  slotCount  slots in use, live or deleted
 * 10   2  freeEnd    where the tuple area starts; 8192 on an empty page
 * 12   2  flags      none defined yet
 * 14  10  reserved   (checksum, page type, ...)
 * </pre>
 * freeStart isn't stored: it is always {@code 24 + 4 * slotCount}, and storing it would give a
 * second value that could disagree with the first.
 *
 * <p><b>Slot</b> (4 bytes): tuple offset (u16) and tuple length (u16). A deleted slot has length
 * {@code 0xFFFF}, the book's "size -1" read as unsigned, which no real tuple can have.
 *
 * <p><b>Delete compacts immediately</b>, as in the book. The tuples stored below the deleted
 * one (the ones inserted after it) slide up to close the hole, and their slots get the new
 * offsets. So free space is always one gap, and an insert never has to compact first. Slot
 * numbers never change, which is why a record ID (page, slot) stays valid while its tuple moves.
 * The consequence for callers: <b>never keep a tuple's offset across a latch release</b>; look it
 * up again through its slot.
 *
 * <p><b>Alignment.</b> Every tuple starts at a multiple of 8, so a {@code long} column at an
 * 8-aligned offset in the tuple is aligned in memory too. A tuple occupies its length rounded
 * up to 8; the slot keeps the exact length. That costs up to 7 bytes per tuple.
 *
 * <p><b>Reusing slots.</b> An insert takes the first deleted slot if there is one, so the slot
 * array doesn't grow forever under insert/delete churn. That means an old record ID can come to
 * point at a new tuple. Fine while nothing keeps record IDs; once indexes do, a slot must only
 * be reused when no index entry can still point at it (Postgres waits for VACUUM).
 *
 * <p><b>Not thread-safe, no latching.</b> The caller holds the page through a {@link PageGuard}:
 * READ to call the reading methods, WRITE to change the page. A READ guard's segment is
 * read-only, so calling {@link #insert} or {@link #delete} through it throws.
 *
 * <p><b>Reused, not allocated per page.</b> Create one, then {@link #wrap} each page you visit.
 * All values are read from the page every time; the flyweight caches nothing but the segment,
 * so it can't go stale when another thread changes the page between two latches.
 */
public final class SlottedPage {

    // Little-endian on purpose: the file format must not depend on the machine. x86 and ARM are
    // little-endian themselves, so it costs nothing there. Unaligned layouts so that any segment
    // works (heap segments over byte[] are only byte-aligned); on x86 and ARM the JIT emits the
    // same instructions either way.
    private static final ValueLayout.OfShort U16 =
            ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong I64 =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    static final int PAGE_LSN = 0;
    static final int SLOT_COUNT = 8;
    static final int FREE_END = 10;
    static final int FLAGS = 12;
    public static final int HEADER_SIZE = 24;
    public static final int SLOT_SIZE = 4;

    public static final int ALIGNMENT = 8;
    /** The longest tuple that fits on an empty page: 8192 - 24 - 4 = 8164, rounded down to 8. */
    public static final int MAX_TUPLE_SIZE = (PAGE_SIZE - HEADER_SIZE - SLOT_SIZE) & -ALIGNMENT;

    /** Slot length marking a deleted slot. */
    static final int DELETED = 0xFFFF;

    private MemorySegment page;

    public SlottedPage() {
    }

    /** Points this flyweight at a page. Doesn't read or change it. */
    public SlottedPage wrap(MemorySegment page) {
        if (page.byteSize() != PAGE_SIZE) {
            throw new IllegalArgumentException("a page is " + PAGE_SIZE + " bytes, got " + page.byteSize());
        }
        this.page = page;
        return this;
    }

    /**
     * Formats the page as empty: no slots, the whole page after the header free. A page fresh
     * from {@link BufferPool#newPage} is all zeros, which is <em>not</em> a valid slotted page
     * (freeEnd would be 0), so call this first.
     */
    public void init() {
        page.set(I64, PAGE_LSN, 0L);
        setSlotCount(0);
        setFreeEnd(PAGE_SIZE);
        page.set(U16, FLAGS, (short) 0);
        for (int i = FLAGS + 2; i < HEADER_SIZE; i++) {
            page.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        }
    }

    // ---------------------------------------------------------------- reading

    /** False for an all-zero page that {@link #init} never ran on: freeEnd is never 0 otherwise. */
    public boolean isFormatted() {
        return freeEnd() != 0;
    }

    /** Slots in use, including deleted ones; valid slot numbers are {@code 0 .. slotCount-1}. */
    public int slotCount() {
        return u16(SLOT_COUNT);
    }

    public boolean isLive(int slot) {
        checkSlot(slot);
        return slotLength(slot) != DELETED;
    }

    /** Where the tuple in {@code slot} starts in the page. Valid until the page changes. */
    public int tupleOffset(int slot) {
        checkLive(slot);
        return slotOffset(slot);
    }

    public int tupleLength(int slot) {
        checkLive(slot);
        return slotLength(slot);
    }

    /**
     * The longest tuple {@link #insert} would accept right now: the free gap, minus a new slot if
     * no deleted slot can be reused, rounded down to the alignment.
     */
    public int freeSpace() {
        int available = gap() - (findDeletedSlot() == -1 ? SLOT_SIZE : 0);
        return available <= 0 ? 0 : available & -ALIGNMENT;
    }

    // ---------------------------------------------------------------- changing

    /**
     * Copies {@code length} bytes from {@code src} into the page as a new tuple.
     *
     * @return the tuple's slot number, or -1 if it doesn't fit (the page is then unchanged).
     *         Not fitting is normal - the caller tries another page - so it isn't an exception.
     * @throws IllegalArgumentException if {@code length} is not between 1 and
     *         {@link #MAX_TUPLE_SIZE}: such a tuple fits on no page at all
     */
    public int insert(MemorySegment src, long srcOffset, int length) {
        if (length < 1 || length > MAX_TUPLE_SIZE) {
            // Zero-length tuples are refused: every real tuple has at least a null bitmap, and a
            // tuple without bytes has no place of its own, which the move-on-delete logic would
            // have to special-case.
            throw new IllegalArgumentException("tuple length must be 1.." + MAX_TUPLE_SIZE + ": " + length);
        }
        int occupied = align(length);
        int slot = findDeletedSlot();
        int needed = occupied + (slot == -1 ? SLOT_SIZE : 0);
        if (gap() < needed) {
            return -1;
        }

        int offset = freeEnd() - occupied;
        MemorySegment.copy(src, srcOffset, page, offset, length);
        for (int i = offset + length; i < offset + occupied; i++) {
            page.set(ValueLayout.JAVA_BYTE, i, (byte) 0); // padding: keep the page deterministic
        }
        if (slot == -1) {
            slot = slotCount();
            setSlotCount(slot + 1);
        }
        setSlot(slot, offset, length);
        setFreeEnd(offset);
        return slot;
    }

    /**
     * Deletes the tuple in {@code slot} and closes the hole it leaves: every tuple stored below
     * it moves up by its size, and their slots get the new offsets. The slot itself stays, marked
     * deleted, so the other slot numbers don't change.
     *
     * <p>Costs one memory move of at most a page, plus one pass over the slot array.
     */
    public void delete(int slot) {
        checkLive(slot);
        int offset = slotOffset(slot);
        int occupied = align(slotLength(slot));
        int freeEnd = freeEnd();

        // Slide [freeEnd, offset) up by `occupied`. Source and target overlap; MemorySegment.copy
        // handles that like C's memmove, without a temporary buffer.
        MemorySegment.copy(page, freeEnd, page, freeEnd + occupied, offset - freeEnd);
        // Zero what was the bottom of the tuple area, so deleted data doesn't linger on disk.
        for (int i = freeEnd; i < freeEnd + occupied; i += Long.BYTES) {
            page.set(I64, i, 0L);
        }

        // Fix the slots of the tuples that moved: those stored below the deleted one. Compare
        // offsets, not slot numbers: with reused slots, a higher slot number doesn't mean a
        // lower offset. Deleted slots have no tuple and must be skipped (their offset is 0).
        int count = slotCount();
        for (int i = 0; i < count; i++) {
            if (slotLength(i) != DELETED) {
                int o = slotOffset(i);
                if (o < offset) {
                    setSlotOffset(i, o + occupied);
                }
            }
        }

        setSlot(slot, 0, DELETED);
        setFreeEnd(freeEnd + occupied);
    }

    // ---------------------------------------------------------------- internals

    private int gap() {
        return freeEnd() - (HEADER_SIZE + SLOT_SIZE * slotCount());
    }

    /** The first deleted slot, or -1. A scan over the slot array: a few hundred entries at most. */
    private int findDeletedSlot() {
        int count = slotCount();
        for (int i = 0; i < count; i++) {
            if (slotLength(i) == DELETED) {
                return i;
            }
        }
        return -1;
    }

    static int align(int length) {
        return (length + ALIGNMENT - 1) & -ALIGNMENT;
    }

    private int freeEnd() {
        return u16(FREE_END);
    }

    private void setFreeEnd(int value) {
        page.set(U16, FREE_END, (short) value);
    }

    private void setSlotCount(int value) {
        page.set(U16, SLOT_COUNT, (short) value);
    }

    private static long slotPos(int slot) {
        return HEADER_SIZE + (long) SLOT_SIZE * slot;
    }

    private int slotOffset(int slot) {
        return u16(slotPos(slot));
    }

    private int slotLength(int slot) {
        return u16(slotPos(slot) + 2);
    }

    private void setSlotOffset(int slot, int offset) {
        page.set(U16, slotPos(slot), (short) offset);
    }

    private void setSlot(int slot, int offset, int length) {
        long pos = slotPos(slot);
        page.set(U16, pos, (short) offset);
        page.set(U16, pos + 2, (short) length);
    }

    /** Java has no unsigned short: values above 32767 (like 0xFFFF) would come back negative. */
    private int u16(long pos) {
        return page.get(U16, pos) & 0xFFFF;
    }

    private void checkSlot(int slot) {
        int count = slotCount();
        if (slot < 0 || slot >= count) {
            throw new IndexOutOfBoundsException("slot " + slot + " of " + count);
        }
    }

    private void checkLive(int slot) {
        checkSlot(slot);
        if (slotLength(slot) == DELETED) {
            throw new IllegalStateException("slot " + slot + " is deleted");
        }
    }
}
