package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class TupleLayoutTest {

    private static Column fixed(String name, ColumnType type) {
        return new Column(name, type, true);
    }

    private static Column varchar(String name) {
        return Column.varchar(name, 50, true);
    }

    private static TupleLayout layout(Column... columns) {
        return new TupleLayout(new Schema(List.of(columns)));
    }

    private static List<Column> bigints(int count) {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            columns.add(fixed("c" + i, ColumnType.BIGINT));
        }
        return columns;
    }

    @Test
    void fixedColumnsAreStoredLargestFirst() {
        TupleLayout layout = layout(
                fixed("flag", ColumnType.BOOLEAN),
                fixed("count", ColumnType.INT),
                fixed("id", ColumnType.BIGINT));

        assertEquals(12, layout.columnOffset(0));
        assertEquals(8, layout.columnOffset(1));
        assertEquals(0, layout.columnOffset(2));
        assertEquals(13, layout.nullBitmapOffset());
        assertEquals(1, layout.nullBitmapSize());
        assertEquals(14, layout.varDataOffset());
    }

    @Test
    void columnsOfTheSameSizeKeepTheirDeclaredOrder() {
        TupleLayout layout = layout(
                fixed("a", ColumnType.DOUBLE),
                fixed("b", ColumnType.BIGINT),
                fixed("c", ColumnType.DOUBLE));

        assertEquals(0, layout.columnOffset(0));
        assertEquals(8, layout.columnOffset(1));
        assertEquals(16, layout.columnOffset(2));
    }

    @Test
    void anOddNumberOfPairsIsPaddedBeforeAnEightByteColumn() {
        TupleLayout layout = layout(varchar("name"), fixed("id", ColumnType.BIGINT));

        assertEquals(0, layout.columnOffset(0));
        assertEquals(8, layout.columnOffset(1));
        assertEquals(16, layout.nullBitmapOffset());
        assertEquals(17, layout.varDataOffset());
    }

    @Test
    void anEvenNumberOfPairsNeedsNoPadding() {
        TupleLayout layout = layout(varchar("a"), varchar("b"), fixed("id", ColumnType.BIGINT));

        assertEquals(0, layout.columnOffset(0));
        assertEquals(4, layout.columnOffset(1));
        assertEquals(8, layout.columnOffset(2));
    }

    @Test
    void paddingOnlyGoesAsFarAsTheLargestFixedColumnNeeds() {
        assertEquals(4, layout(varchar("name"), fixed("n", ColumnType.INT)).columnOffset(1));
        assertEquals(4, layout(varchar("name"), fixed("b", ColumnType.BOOLEAN)).columnOffset(1));
    }

    @Test
    void varcharOnlyRowsHavePairsThenTheBitmap() {
        TupleLayout layout = layout(varchar("a"), varchar("b"));

        assertEquals(8, layout.nullBitmapOffset());
        assertEquals(9, layout.varDataOffset());
    }

    @Test
    void columnsAreAddressedByDeclaredIndexWhateverTheirStoredOrder() {
        TupleLayout layout = layout(
                varchar("name"),
                fixed("active", ColumnType.BOOLEAN),
                fixed("id", ColumnType.BIGINT));

        assertTrue(layout.isVariable(0));
        assertFalse(layout.isVariable(1));
        assertFalse(layout.isVariable(2));
        assertEquals(0, layout.columnOffset(0));
        assertEquals(16, layout.columnOffset(1));
        assertEquals(8, layout.columnOffset(2));
        assertEquals(17, layout.nullBitmapOffset());
        assertEquals(18, layout.varDataOffset());
        assertEquals(3, layout.columnCount());
    }

    @Test
    void theBitmapHasOneBitPerColumnEvenForNotNullColumns() {
        TupleLayout notNull = layout(new Column("id", ColumnType.BIGINT, false));
        assertEquals(1, notNull.nullBitmapSize());

        assertEquals(1, new TupleLayout(new Schema(bigints(8))).nullBitmapSize());
        assertEquals(2, new TupleLayout(new Schema(bigints(9))).nullBitmapSize());
        assertEquals(2, new TupleLayout(new Schema(bigints(16))).nullBitmapSize());
        assertEquals(3, new TupleLayout(new Schema(bigints(17))).nullBitmapSize());
    }

    @Test
    void theFixedPartHasToFitOnAPage() {
        // 1004 * 8 + 126 bytes of bitmap = 8158; one more column makes 8166, over the 8160 limit.
        assertEquals(8158, new TupleLayout(new Schema(bigints(1004))).varDataOffset());
        assertThrows(IllegalArgumentException.class, () -> new TupleLayout(new Schema(bigints(1005))));
    }

    @Test
    void varcharLimitsMayAddUpToMoreThanAPage() {
        TupleLayout layout = layout(Column.varchar("a", 10_000, true), Column.varchar("b", 10_000, true));

        assertEquals(9, layout.varDataOffset());
    }

    @Test
    void randomSchemasAreAlignedAndNothingOverlaps() {
        ColumnType[] types = ColumnType.values();
        Random random = new Random(42);
        for (int round = 0; round < 2_000; round++) {
            int count = 1 + random.nextInt(40);
            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                ColumnType type = types[random.nextInt(types.length)];
                columns.add(type.isVariable() ? varchar("c" + i) : fixed("c" + i, type));
            }
            TupleLayout layout = new TupleLayout(new Schema(columns));

            boolean[] used = new boolean[layout.varDataOffset()];
            for (int i = 0; i < count; i++) {
                ColumnType type = columns.get(i).type();
                int size = type.isVariable() ? TupleLayout.VAR_PAIR_SIZE : type.size();
                int offset = layout.columnOffset(i);
                assertEquals(0, offset % size, "column " + i + " is not aligned in round " + round);
                claim(used, offset, size);
            }
            claim(used, layout.nullBitmapOffset(), layout.nullBitmapSize());
            assertEquals(layout.nullBitmapOffset() + layout.nullBitmapSize(), layout.varDataOffset());
        }
    }

    private static void claim(boolean[] used, int offset, int size) {
        for (int i = offset; i < offset + size; i++) {
            assertFalse(used[i], "byte " + i + " is used twice");
            used[i] = true;
        }
    }
}
