package com.berker.db.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A table's columns in declared order, which is the order SQL sees them in. How they are arranged
 * in a tuple is {@link TupleLayout}'s job. Immutable, and cold path: ordinary Java is fine here.
 *
 * <p>Names are matched exactly. Whether {@code Name} and {@code name} are the same column is a
 * question for the parser, which doesn't exist yet.
 */
public final class Schema {

    private final List<Column> columns;
    private final Map<String, Integer> indexByName = new HashMap<>();

    public Schema(List<Column> columns) {
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("a schema needs at least one column");
        }
        this.columns = List.copyOf(columns);
        for (int i = 0; i < this.columns.size(); i++) {
            String name = this.columns.get(i).name();
            if (indexByName.put(name, i) != null) {
                throw new IllegalArgumentException("duplicate column: " + name);
            }
        }
    }

    public int columnCount() {
        return columns.size();
    }

    public Column column(int index) {
        return columns.get(index);
    }

    public List<Column> columns() {
        return columns;
    }

    /** The column's index in declared order, or -1 if there is no such column. */
    public int indexOf(String name) {
        return indexByName.getOrDefault(name, -1);
    }
}
