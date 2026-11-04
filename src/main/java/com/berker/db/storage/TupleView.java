package com.berker.db.storage;

import static com.berker.db.storage.TupleLayout.F64;
import static com.berker.db.storage.TupleLayout.I32;
import static com.berker.db.storage.TupleLayout.I64;
import static com.berker.db.storage.TupleLayout.U16;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A flyweight over one tuple: {@link #wrap} points it at a row's bytes, and the getters read
 * columns straight from memory using the {@link TupleLayout}. Nothing is copied or decoded, and
 * no row object exists, so a scan over a million rows reuses one view.
 *
 * <p>Columns are addressed by declared index. Getters check the column's type, because reading
 * an INT as a BIGINT would otherwise return garbage quietly. They don't check for NULL: a NULL
 * column keeps zero bytes, so call {@link #isNull} first wherever the column is nullable.
 *
 * <p>The view is only as good as the memory under it. If that memory is a page, it is valid
 * until the latch is released; {@link TableScan} and {@link TableHeap#read} avoid this by
 * handing out private copies. Not thread-safe: one view per worker.
 */
public final class TupleView {

    private final TupleLayout layout;
    private MemorySegment segment;
    private long start;
    private int length;

    public TupleView(TupleLayout layout) {
        this.layout = layout;
    }

    /** Points this view at the tuple of {@code length} bytes at {@code offset} in {@code segment}. */
    public TupleView wrap(MemorySegment segment, long offset, int length) {
        if (length < layout.varDataOffset()) {
            throw new IllegalArgumentException("a tuple of this schema has at least "
                    + layout.varDataOffset() + " bytes, got " + length);
        }
        this.segment = segment;
        this.start = offset;
        this.length = length;
        return this;
    }

    public int length() {
        return length;
    }

    public boolean isNull(int column) {
        if (column < 0 || column >= layout.columnCount()) {
            throw new IndexOutOfBoundsException("column " + column + " of " + layout.columnCount());
        }
        byte bits = segment.get(ValueLayout.JAVA_BYTE, start + layout.nullBitmapOffset() + (column >>> 3));
        return (bits & (1 << (column & 7))) != 0;
    }

    public boolean getBoolean(int column) {
        check(column, ColumnType.BOOLEAN);
        return segment.get(ValueLayout.JAVA_BYTE, start + layout.columnOffset(column)) != 0;
    }

    public int getInt(int column) {
        check(column, ColumnType.INT);
        return segment.get(I32, start + layout.columnOffset(column));
    }

    public long getLong(int column) {
        check(column, ColumnType.BIGINT);
        return segment.get(I64, start + layout.columnOffset(column));
    }

    public double getDouble(int column) {
        check(column, ColumnType.DOUBLE);
        return segment.get(F64, start + layout.columnOffset(column));
    }

    /** The bytes of a VARCHAR are at {@link #varcharOffset} in {@link #segment}. */
    public int varcharLength(int column) {
        check(column, ColumnType.VARCHAR);
        return segment.get(U16, start + layout.columnOffset(column) + 2) & 0xFFFF;
    }

    /** Where the VARCHAR's bytes start in {@link #segment}: the pair's offset is tuple-relative. */
    public long varcharOffset(int column) {
        check(column, ColumnType.VARCHAR);
        return start + (segment.get(U16, start + layout.columnOffset(column)) & 0xFFFF);
    }

    public MemorySegment segment() {
        return segment;
    }

    /**
     * Compares a VARCHAR with other bytes, without decoding either: negative if this column sorts
     * first, 0 if they are equal, positive if it sorts last. Bytes compare as unsigned, and a
     * prefix sorts before the longer value. For UTF-8 that is exactly code point order, which is
     * not a language's alphabetical order; collations can come later.
     */
    public int compareVarchar(int column, MemorySegment other, long otherOffset, int otherLength) {
        long offset = varcharOffset(column);
        int thisLength = varcharLength(column);
        long at = MemorySegment.mismatch(segment, offset, offset + thisLength,
                other, otherOffset, otherOffset + otherLength);
        if (at == -1) {
            return 0;
        }
        if (at == thisLength || at == otherLength) {
            return Integer.compare(thisLength, otherLength);
        }
        return Byte.compareUnsigned(segment.get(ValueLayout.JAVA_BYTE, offset + at),
                other.get(ValueLayout.JAVA_BYTE, otherOffset + at));
    }

    private void check(int column, ColumnType expected) {
        if (layout.columnType(column) != expected) {
            throw new IllegalArgumentException("column " + column + " is "
                    + layout.columnType(column) + ", not " + expected);
        }
    }
}
