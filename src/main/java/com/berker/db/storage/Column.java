package com.berker.db.storage;

/**
 * One column definition.
 *
 * @param maxLength for VARCHAR, the most <em>bytes</em> a value may have (not characters: values
 *                  are stored and compared as UTF-8 bytes, never decoded); 0 for other types
 */
public record Column(String name, ColumnType type, int maxLength, boolean nullable) {

    public Column {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("a column needs a name");
        }
        if (type == null) {
            throw new IllegalArgumentException("column " + name + " needs a type");
        }
        if (type.isVariable() ? maxLength < 1 : maxLength != 0) {
            throw new IllegalArgumentException("column " + name + " of type " + type
                    + " can't have max length " + maxLength);
        }
    }

    /** A fixed-length column. */
    public Column(String name, ColumnType type, boolean nullable) {
        this(name, type, 0, nullable);
    }

    public static Column varchar(String name, int maxLength, boolean nullable) {
        return new Column(name, ColumnType.VARCHAR, maxLength, nullable);
    }
}
