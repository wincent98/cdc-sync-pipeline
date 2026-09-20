package com.fta.cdc;

import com.fta.cdc.schema.ColumnDef;
import com.fta.cdc.schema.ColumnType;
import com.fta.cdc.schema.SchemaRegistry;
import com.fta.cdc.schema.TableSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaRegistryTest {

    private SchemaRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SchemaRegistry();
    }

    private static TableSchema schema(String table, long version, String... columns) {
        Map<String, ColumnDef> cols = new LinkedHashMap<>();
        for (String c : columns) {
            cols.put(c, new ColumnDef(c, ColumnType.varchar(16)));
        }
        return new TableSchema(table, version, cols);
    }

    @Test
    void returnsTheRegisteredSchema() {
        registry.register(schema("orders", 1L, "id"));
        assertEquals(1L, registry.current("orders").schemaVersion());
    }

    @Test
    void throwsForAnUnknownTable() {
        assertThrows(IllegalStateException.class, () -> registry.current("orders"));
    }

    @Test
    void knowsWhichTablesAreRegistered() {
        assertFalse(registry.knows("orders"));
        registry.register(schema("orders", 1L, "id"));
        assertTrue(registry.knows("orders"));
    }

    @Test
    void reRegisteringReplacesTheDefinition() {
        registry.register(schema("orders", 1L, "id"));
        registry.register(schema("orders", 2L, "id", "note"));
        assertEquals(2L, registry.current("orders").schemaVersion());
        assertTrue(registry.current("orders").hasColumn("note"));
    }

    @Test
    void generationAdvancesOnEveryRegister() {
        long before = registry.generation();
        registry.register(schema("orders", 1L, "id"));
        registry.register(schema("orders", 2L, "id"));
        assertEquals(before + 2, registry.generation());
    }

    @Test
    void keepsTablesIndependent() {
        registry.register(schema("orders", 1L, "id"));
        registry.register(schema("items", 9L, "sku"));
        assertEquals(1L, registry.current("orders").schemaVersion());
        assertEquals(9L, registry.current("items").schemaVersion());
    }
}
