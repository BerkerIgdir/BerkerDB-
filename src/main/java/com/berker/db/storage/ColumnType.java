package com.berker.db.storage;

/**
 * The column types the storage layer knows. Fixed-length types have a size in bytes, and it is
 * always a power of two, so a value of that size can be aligned to it.
 */
public enum ColumnType {
    BOOLEAN(1),
    INT(4),
    BIGINT(8),
    DOUBLE(8),
    /** UTF-8 bytes. The value's length differs per row, so it has no fixed size. */
    VARCHAR(0);

    private final int size;

    ColumnType(int size) {
        this.size = size;
    }

    public boolean isVariable() {
        return size == 0;
    }

    /** Bytes one value takes in a tuple; 0 for a variable-length type. */
    public int size() {
        return size;
    }
}
