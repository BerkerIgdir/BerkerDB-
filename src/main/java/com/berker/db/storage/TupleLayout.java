package com.berker.db.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Where each column of a {@link Schema} lives in a tuple (Silberschatz §13.2.1). Computed once per
 * table, then read for every column of every row, so the answers sit in primitive arrays and the
 * accessors allocate nothing.
 *
 * <pre>
 *  [ (offset,length) per var column ][ pad ][ fixed columns ][ null bitmap ][ var bytes ]
 *  0                                                                        varDataOffset
 * </pre>
 *
 * <p>Example: {@code (name VARCHAR(50), active BOOLEAN, id BIGINT)}
 * <pre>
 *  0  name's pair     4  pad     8  id     16  active     17  bitmap     18  name's bytes
 * </pre>
 *
 * <p><b>Pairs</b> are two u16s, offset then length. The offset is relative to the tuple's start,
 * not the page's: delete moves whole tuples around the page, and this way the moved bytes stay
 * correct without being rewritten.
 *
 * <p><b>Fixed columns are stored largest first</b>, not in declared order. {@link SlottedPage}
 * starts every tuple at a multiple of 8, so a position that is a multiple of a column's size in
 * the tuple is one in memory too. Since all sizes are powers of two, going from largest to
 * smallest never needs padding between columns; it only needs the first one to start aligned.
 * That is what the pad after the pairs is for: pairs come in 4s, so a BIGINT right after an odd
 * number of them would start at 4 mod 8. Ties keep their declared order.
 *
 * <p><b>The null bitmap</b> has one bit for every column, nullable or not, so a column's bit is
 * found from its index alone: byte {@code i / 8}, bit {@code i % 8}. The book doesn't say which;
 * covering only nullable columns would save a few bytes on wide tables and need another mapping.
 * It sits after the fixed columns as in the book, which also means it needs no alignment.
 *
 * <p>Only the fixed part has to fit on a page, because its size is the same for every row. The
 * declared VARCHAR limits may add up to more than a page: most rows won't reach them, and a row
 * that really is too long is refused when it is inserted.
 */
public final class TupleLayout {

    public static final int VAR_PAIR_SIZE = 4;

    private final Schema schema;
    private final int[] offsets;
    private final boolean[] variable;
    private final int nullBitmapOffset;
    private final int nullBitmapSize;
    private final int varDataOffset;

    public TupleLayout(Schema schema) {
        this.schema = schema;
        int count = schema.columnCount();
        offsets = new int[count];
        variable = new boolean[count];

        int pos = 0;
        List<Integer> fixed = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            variable[i] = schema.column(i).type().isVariable();
            if (variable[i]) {
                offsets[i] = pos;
                pos += VAR_PAIR_SIZE;
            } else {
                fixed.add(i);
            }
        }

        // List.sort is stable, so columns of the same size stay in declared order.
        fixed.sort(Comparator.comparingInt((Integer i) -> schema.column(i).type().size()).reversed());
        if (!fixed.isEmpty()) {
            int largest = schema.column(fixed.getFirst()).type().size();
            pos = (pos + largest - 1) & -largest;
        }
        for (int i : fixed) {
            offsets[i] = pos;
            pos += schema.column(i).type().size();
        }

        nullBitmapOffset = pos;
        nullBitmapSize = (count + 7) / 8;
        varDataOffset = nullBitmapOffset + nullBitmapSize;
        if (varDataOffset > SlottedPage.MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException("the fixed part of a row takes " + varDataOffset
                    + " bytes, more than the " + SlottedPage.MAX_TUPLE_SIZE + " that fit on a page");
        }
    }

    public Schema schema() {
        return schema;
    }

    public int columnCount() {
        return offsets.length;
    }

    public boolean isVariable(int column) {
        return variable[column];
    }

    /**
     * Where column {@code column} (by declared index) is in the tuple: its value for a fixed-length
     * column, its (offset, length) pair for a variable-length one.
     */
    public int columnOffset(int column) {
        return offsets[column];
    }

    public int nullBitmapOffset() {
        return nullBitmapOffset;
    }

    public int nullBitmapSize() {
        return nullBitmapSize;
    }

    /** Where the variable-length bytes start, which is also the size of a row without any. */
    public int varDataOffset() {
        return varDataOffset;
    }
}
