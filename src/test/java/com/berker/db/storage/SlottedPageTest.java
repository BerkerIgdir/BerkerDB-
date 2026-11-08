package com.berker.db.storage;

import static com.berker.db.storage.DiskManager.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SlottedPageTest {

    private Arena arena;
    private MemorySegment memory;
    private final SlottedPage page = new SlottedPage();

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        memory = arena.allocate(PAGE_SIZE, 8);
        page.wrap(memory).init();
    }

    @AfterEach
    void tearDown() {
        arena.close();
    }

    /** A tuple of {@code length} bytes, every byte equal to {@code fill}. */
    private MemorySegment tuple(int length, int fill) {
        MemorySegment t = arena.allocate(Math.max(length, 1));
        t.fill((byte) fill);
        return t;
    }

    private int insert(int length, int fill) {
        return page.insert(tuple(length, fill), 0, length);
    }

    private byte[] read(int slot) {
        return memory.asSlice(page.tupleOffset(slot), page.tupleLength(slot)).toArray(ValueLayout.JAVA_BYTE);
    }

    private static byte[] filled(int length, int fill) {
        byte[] b = new byte[length];
        Arrays.fill(b, (byte) fill);
        return b;
    }

    private int freeEnd() {
        return memory.get(ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN), SlottedPage.FREE_END) & 0xFFFF;
    }

    @Test
    void emptyPageHasTheWholePageFree() {
        assertEquals(0, page.slotCount());
        assertEquals(PAGE_SIZE, freeEnd());
        assertEquals(SlottedPage.MAX_TUPLE_SIZE, page.freeSpace());
        assertEquals(8160, SlottedPage.MAX_TUPLE_SIZE);
    }

    @Test
    void headerIsLittleEndianAtFixedOffsets() {
        insert(10, 1);
        // Raw bytes, as another machine would read the file: slotCount = 1, freeEnd = 8192 - 16.
        assertEquals(1, memory.get(ValueLayout.JAVA_BYTE, 8));
        assertEquals(0, memory.get(ValueLayout.JAVA_BYTE, 9));
        int freeEnd = PAGE_SIZE - 16;
        assertEquals((byte) freeEnd, memory.get(ValueLayout.JAVA_BYTE, 10));
        assertEquals((byte) (freeEnd >>> 8), memory.get(ValueLayout.JAVA_BYTE, 11));
    }

    @Test
    void insertedTuplesReadBackAndGrowDownFromTheEnd() {
        int a = insert(100, 0xA);
        int b = insert(30, 0xB);
        assertEquals(0, a);
        assertEquals(1, b);
        assertEquals(2, page.slotCount());
        assertEquals(PAGE_SIZE - 104, page.tupleOffset(a)); // 100 rounded up to 104
        assertEquals(PAGE_SIZE - 104 - 32, page.tupleOffset(b));
        assertEquals(100, page.tupleLength(a));
        assertEquals(30, page.tupleLength(b));
        assertArrayEquals(filled(100, 0xA), read(a));
        assertArrayEquals(filled(30, 0xB), read(b));
    }

    @Test
    void tuplesStartAtMultiplesOfEightAndPaddingIsZero() {
        int s = insert(13, 0xFF);
        int offset = page.tupleOffset(s);
        assertEquals(0, offset % SlottedPage.ALIGNMENT);
        for (int i = offset + 13; i < offset + 16; i++) {
            assertEquals(0, memory.get(ValueLayout.JAVA_BYTE, i), "padding byte " + i);
        }
    }

    @Test
    void insertCopiesFromTheGivenSourceOffset() {
        MemorySegment src = arena.allocate(20);
        for (int i = 0; i < 20; i++) {
            src.set(ValueLayout.JAVA_BYTE, i, (byte) i);
        }
        int s = page.insert(src, 5, 3);
        assertArrayEquals(new byte[] {5, 6, 7}, read(s));
    }

    @Test
    void freeSpaceIsExactlyTheLargestTupleThatFits() {
        insert(1000, 1);
        int free = page.freeSpace();
        byte[] before = memory.toArray(ValueLayout.JAVA_BYTE);
        assertEquals(-1, insert(free + 1, 2), "one byte more than freeSpace() doesn't fit");
        assertArrayEquals(before, memory.toArray(ValueLayout.JAVA_BYTE), "a refused insert changes nothing");
        assertEquals(1, insert(free, 2));
        assertEquals(0, page.freeSpace());
    }

    @Test
    void largestTupleFillsAnEmptyPage() {
        int s = insert(SlottedPage.MAX_TUPLE_SIZE, 7);
        assertEquals(0, s);
        assertEquals(0, page.freeSpace());
        assertEquals(-1, insert(1, 8));
        assertArrayEquals(filled(SlottedPage.MAX_TUPLE_SIZE, 7), read(s));
    }

    @Test
    void lengthsOutsideOneToMaxAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> insert(0, 1));
        assertThrows(IllegalArgumentException.class, () -> insert(SlottedPage.MAX_TUPLE_SIZE + 1, 1));
    }

    @Test
    void deleteSlidesLaterTuplesUpAndKeepsTheirSlots() {
        int a = insert(100, 0xA);
        int b = insert(300, 0xB);
        int c = insert(200, 0xC);
        int d = insert(50, 0xD);
        int aOffset = page.tupleOffset(a);
        int cOffset = page.tupleOffset(c);
        int dOffset = page.tupleOffset(d);
        int freeEndBefore = freeEnd();

        page.delete(b);

        assertFalse(page.isLive(b));
        assertEquals(4, page.slotCount(), "the slot stays, marked deleted");
        assertEquals(aOffset, page.tupleOffset(a), "stored above b: doesn't move");
        assertEquals(cOffset + 304, page.tupleOffset(c), "stored below b: moves up by b's size");
        assertEquals(dOffset + 304, page.tupleOffset(d));
        assertEquals(freeEndBefore + 304, freeEnd(), "free space is one gap again");
        assertArrayEquals(filled(100, 0xA), read(a));
        assertArrayEquals(filled(200, 0xC), read(c));
        assertArrayEquals(filled(50, 0xD), read(d));
    }

    @Test
    void deletedBytesAreZeroed() {
        insert(100, 0xA);
        int b = insert(40, 0xB);
        int freeEndBefore = freeEnd();
        page.delete(0);
        // The old bottom of the tuple area (now free) must not keep tuple bytes.
        for (int i = freeEndBefore; i < freeEnd(); i++) {
            assertEquals(0, memory.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
        }
        assertArrayEquals(filled(40, 0xB), read(b));
    }

    @Test
    void spaceFreedByADeleteIsUsableAtOnce() {
        insert(4000, 1);
        int big = insert(4000, 2);
        assertEquals(-1, insert(1000, 3), "page is full");
        page.delete(big);
        // With lazy compaction this insert would need a compaction first; here the gap is whole.
        assertTrue(insert(1000, 3) >= 0);
    }

    @Test
    void insertReusesTheFirstDeletedSlot() {
        insert(10, 1);
        insert(10, 2);
        insert(10, 3);
        page.delete(1);
        page.delete(0);
        assertEquals(0, insert(10, 4));
        assertEquals(1, insert(10, 5));
        assertEquals(3, insert(10, 6), "no deleted slot left: a new one");
        assertEquals(4, page.slotCount());
    }

    /**
     * With a reused slot, slot order and offset order differ: slot 0 now holds the newest
     * tuple, stored lowest. Deleting slot 2 must move slot 0's tuple, although its slot number
     * is lower. A delete that compared slot numbers instead of offsets would get this wrong.
     */
    @Test
    void deleteComparesOffsetsNotSlotNumbers() {
        insert(16, 1);
        insert(16, 2);
        insert(16, 3);
        page.delete(0);
        assertEquals(0, insert(24, 4)); // reuses slot 0, stored below slot 2
        int before = page.tupleOffset(0);
        page.delete(2);
        assertEquals(before + 16, page.tupleOffset(0));
        assertArrayEquals(filled(24, 4), read(0));
        assertArrayEquals(filled(16, 2), read(1));
    }

    @Test
    void freeSpaceCountsAReusableSlot() {
        insert(16, 1);
        int s = insert(16, 2);
        // gap = 8160 (freeEnd) - 32 (header + 2 slots) = 8128; a new slot takes 4: 8124 -> 8120.
        assertEquals(8120, page.freeSpace());
        page.delete(s);
        // The tuple's 16 bytes come back (gap 8144), and the deleted slot will be reused, so
        // nothing is set aside for a new one.
        assertEquals(8144, page.freeSpace());
    }

    @Test
    void deletedAndMissingSlotsAreRefused() {
        int s = insert(10, 1);
        page.delete(s);
        assertThrows(IllegalStateException.class, () -> page.delete(s));
        assertThrows(IllegalStateException.class, () -> page.tupleOffset(s));
        assertThrows(IllegalStateException.class, () -> page.tupleLength(s));
        assertThrows(IndexOutOfBoundsException.class, () -> page.isLive(1));
        assertThrows(IndexOutOfBoundsException.class, () -> page.isLive(-1));
    }

    @Test
    void aReadOnlyPageCanBeReadButNotChanged() {
        int s = insert(10, 1);
        SlottedPage readOnly = new SlottedPage().wrap(memory.asReadOnly());
        assertEquals(10, readOnly.tupleLength(s));
        assertThrows(IllegalArgumentException.class, () -> readOnly.insert(tuple(5, 2), 0, 5));
        assertThrows(IllegalArgumentException.class, () -> readOnly.delete(s));
        assertArrayEquals(filled(10, 1), read(s), "the failed delete changed nothing");
    }

    private boolean update(int slot, int length, int fill) {
        return page.update(slot, tuple(length, fill), 0, length);
    }

    /** The free gap between the slot array and the tuples, before setting aside any new slot. */
    private int gap() {
        return freeEnd() - SlottedPage.HEADER_SIZE - SlottedPage.SLOT_SIZE * page.slotCount();
    }

    @Test
    void anUpdateOfTheSameRoundedSizeStaysInPlace() {
        insert(100, 0xA);
        int b = insert(13, 0xB);
        int offset = page.tupleOffset(b);
        int freeEndBefore = freeEnd();

        assertTrue(update(b, 10, 0xC)); // 13 and 10 both take 16 bytes

        assertEquals(offset, page.tupleOffset(b));
        assertEquals(freeEndBefore, freeEnd());
        assertArrayEquals(filled(10, 0xC), read(b));
        for (int i = offset + 10; i < offset + 16; i++) {
            assertEquals(0, memory.get(ValueLayout.JAVA_BYTE, i), "padding byte " + i);
        }
    }

    @Test
    void aLargerUpdateMovesTheTupleWithinThePageAndKeepsTheSlot() {
        int a = insert(100, 0xA);
        int b = insert(40, 0xB);
        int c = insert(60, 0xC);
        int freeEndBefore = freeEnd();

        assertTrue(update(b, 500, 0xD));

        assertEquals(3, page.slotCount(), "no new slot");
        assertEquals(freeEndBefore + 40 - 504, freeEnd(), "old 40 bytes back, new 504 used");
        assertEquals(freeEnd(), page.tupleOffset(b), "the new version is the lowest tuple");
        assertArrayEquals(filled(100, 0xA), read(a));
        assertArrayEquals(filled(500, 0xD), read(b));
        assertArrayEquals(filled(60, 0xC), read(c));
    }

    @Test
    void aSmallerUpdateGivesSpaceBack() {
        insert(100, 0xA);
        int b = insert(1000, 0xB);
        int free = page.freeSpace();

        assertTrue(update(b, 200, 0xC));

        assertEquals(free + 800, page.freeSpace());
        assertArrayEquals(filled(200, 0xC), read(b));
    }

    @Test
    void anUpdateCanUseTheTuplesOwnOldSpace() {
        int a = insert(4000, 0xA);
        int b = insert(4000, 0xB);
        int largest = 4000 + gap(); // more than freeSpace(): the old 4000 bytes come back first

        assertTrue(largest > page.freeSpace());
        assertTrue(update(b, largest, 0xC));

        assertEquals(0, gap());
        assertArrayEquals(filled(largest, 0xC), read(b));
        assertArrayEquals(filled(4000, 0xA), read(a));
    }

    @Test
    void anUpdateThatDoesNotFitChangesNothing() {
        int a = insert(4000, 0xA);
        insert(4000, 0xB);
        byte[] before = memory.toArray(ValueLayout.JAVA_BYTE);

        assertFalse(update(a, 4000 + gap() + 1, 0xC));

        assertArrayEquals(before, memory.toArray(ValueLayout.JAVA_BYTE));
    }

    @Test
    void updatesOfDeletedOrMissingSlotsAndBadLengthsAreRefused() {
        int s = insert(10, 1);
        int d = insert(10, 2);
        page.delete(d);

        assertThrows(IllegalStateException.class, () -> update(d, 10, 3));
        assertThrows(IndexOutOfBoundsException.class, () -> update(5, 10, 3));
        assertThrows(IllegalArgumentException.class, () -> page.update(s, tuple(1, 3), 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> page.update(s, tuple(1, 3), 0, SlottedPage.MAX_TUPLE_SIZE + 1));
        assertArrayEquals(filled(10, 1), read(s));
    }

    @Test
    void wrapRejectsSegmentsThatAreNotAPage() {
        assertThrows(IllegalArgumentException.class, () -> new SlottedPage().wrap(arena.allocate(100)));
    }

    /**
     * Random inserts, updates and deletes against a plain map from slot to contents. After every step:
     * each live tuple reads back intact, tuples don't overlap, are aligned, sit between freeEnd
     * and the page end, and fill that area exactly (so free space really is one gap).
     */
    @Test
    void randomInsertsAndDeletesMatchAModel() {
        Random random = new Random(42);
        Map<Integer, byte[]> model = new HashMap<>();
        for (int step = 0; step < 20_000; step++) {
            int action = model.isEmpty() ? 0 : random.nextInt(100);
            if (action >= 40 && action < 60) {
                List<Integer> live = new ArrayList<>(model.keySet());
                int slot = live.get(random.nextInt(live.size()));
                int length = 1 + random.nextInt(random.nextBoolean() ? 64 : 1500);
                byte[] data = new byte[length];
                random.nextBytes(data);
                boolean fits = ((length + 7) & -8) <= gap() + ((page.tupleLength(slot) + 7) & -8);
                boolean updated = page.update(slot, MemorySegment.ofArray(data), 0, length);
                assertEquals(fits, updated, "update fit, step " + step);
                if (updated) {
                    model.put(slot, data);
                }
            } else if (action < 55) {
                int length = 1 + random.nextInt(random.nextBoolean() ? 64 : 1500);
                byte[] data = new byte[length];
                random.nextBytes(data);
                MemorySegment src = MemorySegment.ofArray(data);
                boolean fits = length <= page.freeSpace();
                int slot = page.insert(src, 0, length);
                assertEquals(fits, slot >= 0, "freeSpace() predicted the insert, step " + step);
                if (slot >= 0) {
                    assertFalse(model.containsKey(slot), "slot " + slot + " handed out while live");
                    model.put(slot, data);
                }
            } else {
                List<Integer> live = new ArrayList<>(model.keySet());
                int slot = live.get(random.nextInt(live.size()));
                page.delete(slot);
                model.remove(slot);
            }
            check(model, step);
        }
    }

    private void check(Map<Integer, byte[]> model, int step) {
        int used = 0;
        int count = page.slotCount();
        List<int[]> ranges = new ArrayList<>();
        for (int s = 0; s < count; s++) {
            assertEquals(model.containsKey(s), page.isLive(s), "slot " + s + " liveness, step " + step);
            if (!page.isLive(s)) {
                continue;
            }
            assertArrayEquals(model.get(s), read(s), "slot " + s + " contents, step " + step);
            int offset = page.tupleOffset(s);
            int occupied = (page.tupleLength(s) + 7) & -8;
            assertEquals(0, offset % 8, "alignment");
            assertTrue(offset >= freeEnd() && offset + occupied <= PAGE_SIZE, "inside the tuple area");
            ranges.add(new int[] {offset, offset + occupied});
            used += occupied;
        }
        ranges.sort((x, y) -> Integer.compare(x[0], y[0]));
        for (int i = 1; i < ranges.size(); i++) {
            assertTrue(ranges.get(i - 1)[1] <= ranges.get(i)[0], "tuples overlap, step " + step);
        }
        assertEquals(PAGE_SIZE - freeEnd(), used, "tuple area has holes, step " + step);
        assertTrue(SlottedPage.HEADER_SIZE + SlottedPage.SLOT_SIZE * count <= freeEnd(), "slots run into tuples");
    }
}
