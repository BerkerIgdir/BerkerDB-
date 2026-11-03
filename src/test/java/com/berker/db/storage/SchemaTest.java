package com.berker.db.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaTest {

    private static final Column ID = new Column("id", ColumnType.BIGINT, false);
    private static final Column NAME = Column.varchar("name", 50, true);

    @Test
    void columnsKeepTheirDeclaredOrder() {
        Schema schema = new Schema(List.of(NAME, ID));

        assertEquals(2, schema.columnCount());
        assertEquals(NAME, schema.column(0));
        assertEquals(ID, schema.column(1));
    }

    @Test
    void indexOfFindsColumnsByExactName() {
        Schema schema = new Schema(List.of(ID, NAME));

        assertEquals(0, schema.indexOf("id"));
        assertEquals(1, schema.indexOf("name"));
        assertEquals(-1, schema.indexOf("ID"));
        assertEquals(-1, schema.indexOf("missing"));
    }

    @Test
    void laterChangesToTheCallersListDoNotReachTheSchema() {
        List<Column> columns = new ArrayList<>(List.of(ID));
        Schema schema = new Schema(columns);

        columns.add(NAME);

        assertEquals(1, schema.columnCount());
        assertThrows(UnsupportedOperationException.class, () -> schema.columns().add(NAME));
    }

    @Test
    void emptySchemasAndDuplicateNamesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Schema(List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new Schema(List.of(ID, new Column("id", ColumnType.INT, true))));
    }

    @Test
    void columnsNeedANameAndAType() {
        assertThrows(IllegalArgumentException.class, () -> new Column(null, ColumnType.INT, false));
        assertThrows(IllegalArgumentException.class, () -> new Column(" ", ColumnType.INT, false));
        assertThrows(IllegalArgumentException.class, () -> new Column("a", null, false));
    }

    @Test
    void onlyVarcharHasAMaxLength() {
        assertEquals(50, NAME.maxLength());
        assertEquals(0, ID.maxLength());
        assertThrows(IllegalArgumentException.class, () -> Column.varchar("a", 0, false));
        assertThrows(IllegalArgumentException.class, () -> new Column("a", ColumnType.VARCHAR, false));
        assertThrows(IllegalArgumentException.class, () -> new Column("a", ColumnType.INT, 4, false));
    }

    @Test
    void fixedSizedTypesArePowersOfTwoAndOnlyVarcharIsVariable() {
        for (ColumnType type : ColumnType.values()) {
            assertEquals(type == ColumnType.VARCHAR, type.isVariable(), type.name());
            if (!type.isVariable()) {
                assertEquals(1, Integer.bitCount(type.size()), type.name());
            }
        }
        assertTrue(ColumnType.VARCHAR.isVariable());
        assertFalse(ColumnType.DOUBLE.isVariable());
    }
}
