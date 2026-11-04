package com.berker.db.storage;

import static com.berker.db.storage.TupleLayout.F64;
import static com.berker.db.storage.TupleLayout.I32;
import static com.berker.db.storage.TupleLayout.I64;
import static com.berker.db.storage.TupleLayout.U16;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Builds one row in the {@link TupleLayout} format, in a buffer it reuses for every row. The
 * opposite of {@link TupleView}:
 *
 * <pre>{@code
 * writer.begin();
 * writer.setLong(0, 42);
 * writer.setVarchar(1, nameBytes, 0, nameLength);
 * int length = writer.finish();
 * heap.insert(context, writer.segment(), 0, length);
 * }</pre>
 *
 * <p>Every column starts as NULL, and setting one clears its null bit. Columns can be set in any
 * order, so VARCHAR bytes are simply appended in the order they arrive; the pairs say where each
 * one ended up. Not thread-safe: one writer per worker.
 */
public final class TupleWriter {

    private final TupleLayout layout;
    private final MemorySegment buffer;
    private int end;

    /** @param buffer the row is built here; needs room for {@link SlottedPage#MAX_TUPLE_SIZE} */
    public TupleWriter(TupleLayout layout, MemorySegment buffer) {
        if (buffer.byteSize() < SlottedPage.MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException("buffer needs " + SlottedPage.MAX_TUPLE_SIZE
                    + " bytes, got " + buffer.byteSize());
        }
        this.layout = layout;
        this.buffer = buffer;
    }

    /** Starts a new row: every byte of the fixed part zero, and every column NULL. */
    public void begin() {
        end = layout.varDataOffset();
        for (int i = 0; i < end; i++) {
            buffer.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        }
        for (int column = 0; column < layout.columnCount(); column++) {
            setNullBit(column, true);
        }
    }

    public void setBoolean(int column, boolean value) {
        check(column, ColumnType.BOOLEAN);
        buffer.set(ValueLayout.JAVA_BYTE, layout.columnOffset(column), (byte) (value ? 1 : 0));
        setNullBit(column, false);
    }

    public void setInt(int column, int value) {
        check(column, ColumnType.INT);
        buffer.set(I32, layout.columnOffset(column), value);
        setNullBit(column, false);
    }

    public void setLong(int column, long value) {
        check(column, ColumnType.BIGINT);
        buffer.set(I64, layout.columnOffset(column), value);
        setNullBit(column, false);
    }

    public void setDouble(int column, double value) {
        check(column, ColumnType.DOUBLE);
        buffer.set(F64, layout.columnOffset(column), value);
        setNullBit(column, false);
    }

    /**
     * Copies a VARCHAR's bytes into the row. Each VARCHAR can be set once per row: its bytes have
     * already been appended, so a second call would leave dead bytes behind.
     *
     * @throws IllegalArgumentException if the value is over the column's limit, or makes the row
     *         too long for a page
     */
    public void setVarchar(int column, MemorySegment src, long srcOffset, int length) {
        check(column, ColumnType.VARCHAR);
        if (!isNullBitSet(column)) {
            throw new IllegalStateException("column " + column + " is already set in this row");
        }
        int maxLength = layout.schema().column(column).maxLength();
        if (length < 0 || length > maxLength) {
            throw new IllegalArgumentException("column " + column + " takes up to " + maxLength
                    + " bytes, got " + length);
        }
        if (end + length > SlottedPage.MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException("row is " + (end + length)
                    + " bytes, more than the " + SlottedPage.MAX_TUPLE_SIZE + " that fit on a page");
        }
        MemorySegment.copy(src, srcOffset, buffer, end, length);
        int pair = layout.columnOffset(column);
        buffer.set(U16, pair, (short) end);
        buffer.set(U16, pair + 2, (short) length);
        end += length;
        setNullBit(column, false);
    }

    /**
     * Ends the row.
     *
     * @return the row's length in bytes
     * @throws IllegalArgumentException if a NOT NULL column wasn't set
     */
    public int finish() {
        for (int column = 0; column < layout.columnCount(); column++) {
            if (!layout.isNullable(column) && isNullBitSet(column)) {
                throw new IllegalArgumentException("column "
                        + layout.schema().column(column).name() + " can't be NULL");
            }
        }
        return end;
    }

    /** The buffer the row is in, starting at offset 0. */
    public MemorySegment segment() {
        return buffer;
    }

    private boolean isNullBitSet(int column) {
        long pos = layout.nullBitmapOffset() + (column >>> 3);
        return (buffer.get(ValueLayout.JAVA_BYTE, pos) & (1 << (column & 7))) != 0;
    }

    private void setNullBit(int column, boolean isNull) {
        long pos = layout.nullBitmapOffset() + (column >>> 3);
        int bits = buffer.get(ValueLayout.JAVA_BYTE, pos);
        int mask = 1 << (column & 7);
        buffer.set(ValueLayout.JAVA_BYTE, pos, (byte) (isNull ? bits | mask : bits & ~mask));
    }

    private void check(int column, ColumnType expected) {
        if (layout.columnType(column) != expected) {
            throw new IllegalArgumentException("column " + column + " is "
                    + layout.columnType(column) + ", not " + expected);
        }
    }
}
