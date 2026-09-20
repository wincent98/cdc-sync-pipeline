package com.fta.cdc;

import com.fta.cdc.schema.ColumnDef;
import com.fta.cdc.schema.ColumnType;
import com.fta.cdc.schema.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableSchemaTest {

    private static TableSchema schema() {
        Map<String, ColumnDef> cols = new LinkedHashMap<>();
        cols.put("id", new ColumnDef("id", ColumnType.intType()));
        cols.put("note", new ColumnDef("note", ColumnType.varchar(32)));
        return new TableSchema("orders", 2L, cols);
    }

    @Test
    void exposesTableAndVersion() {
        assertEquals("orders", schema().table());
        assertEquals(2L, schema().schemaVersion());
    }

    @Test
    void looksUpColumns() {
        assertTrue(schema().hasColumn("note"));
        assertFalse(schema().hasColumn("currency"));
        assertNull(schema().column("currency"));
    }

    @Test
    void keepsColumnOrder() {
        assertEquals("[id, note]", schema().columnNames().toString());
    }

    @Test
    void exposesAnImmutableColumnMap() {
        TableSchema s = schema();
        assertThrows(UnsupportedOperationException.class,
                () -> s.columns().put("x", new ColumnDef("x", ColumnType.intType())));
    }

    @Test
    void columnCarriesItsType() {
        assertEquals(ColumnType.varchar(32), schema().column("note").type());
    }

    @Test
    void rejectsAnEmptyColumnName() {
        assertThrows(IllegalArgumentException.class, () -> new ColumnDef("", ColumnType.intType()));
    }
}
