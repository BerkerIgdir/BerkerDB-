package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests {@link TupleWriter} and {@link TupleView} together: what one writes the other reads. */
class TupleViewTest {

    private static final int NAME = 0;
    private static final int ACTIVE = 1;
    private static final int ID = 2;
    private static final int SCORE = 3;
    private static final int AGE = 4;
    private static final int CITY = 5;

    private static final TupleLayout LAYOUT = new TupleLayout(new Schema(List.of(
            Column.varchar("name", 50, true),
            new Column("active", ColumnType.BOOLEAN, true),
            new Column("id", ColumnType.BIGINT, false),
            new Column("score", ColumnType.DOUBLE, true),
            new Column("age", ColumnType.INT, true),
            Column.varchar("city", 20, true))));

    private Arena arena;
    private TupleWriter writer;
    private final TupleView view = new TupleView(LAYOUT);

    @BeforeEach
    void setUp() {
        arena = Arena.ofConfined();
        writer = new TupleWriter(LAYOUT, arena.allocate(SlottedPage.MAX_TUPLE_SIZE, 8));
    }

    @AfterEach
    void tearDown() {
        arena.close();
    }

    private MemorySegment utf8(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment segment = arena.allocate(Math.max(bytes.length, 1));
        MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return segment;
    }

    private void setVarchar(TupleWriter target, int column, String value) {
        target.setVarchar(column, utf8(value), 0, value.getBytes(StandardCharsets.UTF_8).length);
    }

    private String varchar(int column) {
        byte[] bytes = view.segment().asSlice(view.varcharOffset(column), view.varcharLength(column))
                .toArray(ValueLayout.JAVA_BYTE);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void wrapRow() {
        view.wrap(writer.segment(), 0, writer.finish());
    }

    private int compare(String a, String b) {
        writer.begin();
        writer.setLong(ID, 1);
        setVarchar(writer, NAME, a);
        wrapRow();
        return view.compareVarchar(NAME, utf8(b), 0, b.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    void everyTypeReadsBackWhatWasWritten() {
        writer.begin();
        setVarchar(writer, CITY, "Ankara");
        writer.setInt(AGE, -33);
        writer.setDouble(SCORE, 2.5);
        writer.setLong(ID, Long.MIN_VALUE);
        writer.setBoolean(ACTIVE, true);
        setVarchar(writer, NAME, "Berker");
        wrapRow();

        assertEquals("Berker", varchar(NAME));
        assertTrue(view.getBoolean(ACTIVE));
        assertEquals(Long.MIN_VALUE, view.getLong(ID));
        assertEquals(2.5, view.getDouble(SCORE));
        assertEquals(-33, view.getInt(AGE));
        assertEquals("Ankara", varchar(CITY));
        for (int column = 0; column < LAYOUT.columnCount(); column++) {
            assertFalse(view.isNull(column));
        }
    }

    @Test
    void columnsNotSetAreNullAndKeepZeroBytes() {
        writer.begin();
        writer.setLong(ID, 1);
        wrapRow();

        assertTrue(view.isNull(NAME));
        assertTrue(view.isNull(AGE));
        assertFalse(view.isNull(ID));
        assertEquals(0, view.getInt(AGE));
        assertEquals(0, view.varcharLength(NAME));
    }

    @Test
    void aRowEndsAfterItsVarcharBytes() {
        writer.begin();
        writer.setLong(ID, 1);
        setVarchar(writer, NAME, "abc");

        assertEquals(LAYOUT.varDataOffset() + 3, writer.finish());
    }

    @Test
    void anEmptyVarcharIsNotNull() {
        writer.begin();
        writer.setLong(ID, 1);
        setVarchar(writer, NAME, "");
        wrapRow();

        assertFalse(view.isNull(NAME));
        assertEquals(0, view.varcharLength(NAME));
    }

    @Test
    void beginForgetsThePreviousRow() {
        writer.begin();
        writer.setLong(ID, 1);
        writer.setInt(AGE, 5);
        setVarchar(writer, NAME, "long name");
        writer.begin();
        writer.setLong(ID, 2);
        wrapRow();

        assertTrue(view.isNull(AGE));
        assertTrue(view.isNull(NAME));
        assertEquals(0, view.getInt(AGE));
        assertEquals(LAYOUT.varDataOffset(), view.length());
    }

    @Test
    void varcharOffsetsAreRelativeSoARowCanMove() {
        writer.begin();
        writer.setLong(ID, 7);
        setVarchar(writer, NAME, "moved");
        int length = writer.finish();
        MemorySegment elsewhere = arena.allocate(1000, 8);
        MemorySegment.copy(writer.segment(), 0, elsewhere, 512, length);

        view.wrap(elsewhere, 512, length);

        assertEquals(7, view.getLong(ID));
        assertEquals("moved", varchar(NAME));
    }

    @Test
    void wrapMovesTheSameViewToAnotherRow() {
        MemorySegment memory = arena.allocate(2 * LAYOUT.varDataOffset(), 8);
        for (int i = 0; i < 2; i++) {
            writer.begin();
            writer.setLong(ID, 100 + i);
            MemorySegment.copy(writer.segment(), 0, memory, (long) i * LAYOUT.varDataOffset(), writer.finish());
        }

        assertEquals(100, view.wrap(memory, 0, LAYOUT.varDataOffset()).getLong(ID));
        assertEquals(101, view.wrap(memory, LAYOUT.varDataOffset(), LAYOUT.varDataOffset()).getLong(ID));
    }

    @Test
    void varcharsCompareAsUnsignedBytes() {
        assertEquals(0, compare("abc", "abc"));
        assertTrue(compare("abc", "abd") < 0);
        assertTrue(compare("abd", "abc") > 0);
        assertTrue(compare("ab", "abc") < 0);
        assertTrue(compare("abc", "ab") > 0);
        assertEquals(0, compare("", ""));
        assertTrue(compare("", "a") < 0);
        // 'ç' is two bytes starting with 0xC3, negative as a signed byte, but it sorts after 'z'.
        assertTrue(compare("ç", "z") > 0);
    }

    @Test
    void comparingUsesTheGivenOffset() {
        writer.begin();
        writer.setLong(ID, 1);
        setVarchar(writer, NAME, "abc");
        wrapRow();

        assertEquals(0, view.compareVarchar(NAME, utf8("xxabc"), 2, 3));
    }

    @Test
    void readingOrWritingTheWrongTypeIsRefused() {
        writer.begin();
        writer.setLong(ID, 1);
        wrapRow();

        assertThrows(IllegalArgumentException.class, () -> view.getInt(ID));
        assertThrows(IllegalArgumentException.class, () -> view.getLong(AGE));
        assertThrows(IllegalArgumentException.class, () -> view.varcharLength(ID));
        assertThrows(IllegalArgumentException.class, () -> writer.setInt(ID, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> view.isNull(LAYOUT.columnCount()));
        assertThrows(IndexOutOfBoundsException.class, () -> view.isNull(-1));
    }

    @Test
    void wrapRefusesATupleShorterThanTheFixedPart() {
        MemorySegment memory = arena.allocate(64);
        assertThrows(IllegalArgumentException.class, () -> view.wrap(memory, 0, LAYOUT.varDataOffset() - 1));
    }

    @Test
    void notNullColumnsMustBeSet() {
        writer.begin();
        writer.setInt(AGE, 1);

        assertThrows(IllegalArgumentException.class, writer::finish);
    }

    @Test
    void aVarcharCanBeSetOnceAndWithinItsLimit() {
        writer.begin();
        setVarchar(writer, CITY, "x".repeat(20));

        assertThrows(IllegalStateException.class, () -> setVarchar(writer, CITY, "y"));
        assertThrows(IllegalArgumentException.class, () -> setVarchar(writer, NAME, "x".repeat(51)));
    }

    @Test
    void aRowTooLongForAPageIsRefusedWhileItIsBuilt() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.varchar("a", 8000, true));
        columns.add(Column.varchar("b", 8000, true));
        TupleLayout layout = new TupleLayout(new Schema(columns));
        TupleWriter wide = new TupleWriter(layout, arena.allocate(SlottedPage.MAX_TUPLE_SIZE));
        wide.begin();
        setVarchar(wide, 0, "x".repeat(8000));

        assertThrows(IllegalArgumentException.class, () -> setVarchar(wide, 1, "y".repeat(8000)));
    }

    @Test
    void theBufferMustHoldTheLargestRow() {
        MemorySegment small = arena.allocate(SlottedPage.MAX_TUPLE_SIZE - 1);
        assertThrows(IllegalArgumentException.class, () -> new TupleWriter(LAYOUT, small));
    }
}
