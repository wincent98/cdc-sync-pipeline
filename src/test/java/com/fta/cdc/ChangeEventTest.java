package com.fta.cdc;

import com.fta.cdc.event.ChangeEvent;
import com.fta.cdc.event.Op;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChangeEventTest {

    private static Map<String, Object> cols() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", 1);
        m.put("amount", 100L);
        return m;
    }

    @Test
    void exposesAllFields() {
        ChangeEvent e = new ChangeEvent(7L, "orders", "O-1", Op.UPDATE, 3L, cols());
        assertEquals(7L, e.offset());
        assertEquals("orders", e.table());
        assertEquals("O-1", e.primaryKey());
        assertEquals(Op.UPDATE, e.op());
        assertEquals(3L, e.version());
    }

    @Test
    void copiesTheColumnMap() {
        Map<String, Object> source = cols();
        ChangeEvent e = new ChangeEvent(1L, "orders", "O-1", Op.INSERT, 1L, source);
        source.put("note", "late");
        assertEquals(2, e.columns().size());
    }

    @Test
    void exposesAnImmutableColumnMap() {
        ChangeEvent e = new ChangeEvent(1L, "orders", "O-1", Op.INSERT, 1L, cols());
        assertThrows(UnsupportedOperationException.class, () -> e.columns().put("x", 1));
    }

    @Test
    void acceptsAnEmptyColumnMap() {
        ChangeEvent e = new ChangeEvent(1L, "orders", "O-1", Op.DELETE, 1L, new LinkedHashMap<>());
        assertTrue(e.columns().isEmpty());
    }

    @Test
    void rejectsAnEmptyTable() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChangeEvent(1L, "", "O-1", Op.INSERT, 1L, cols()));
    }

    @Test
    void rejectsAnEmptyPrimaryKey() {
        assertThrows(IllegalArgumentException.class,
                () -> new ChangeEvent(1L, "orders", "", Op.INSERT, 1L, cols()));
    }
}
