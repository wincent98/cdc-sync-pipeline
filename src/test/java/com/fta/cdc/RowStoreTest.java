package com.fta.cdc;

import com.fta.cdc.sink.Row;
import com.fta.cdc.sink.RowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowStoreTest {

    private RowStore store;

    @BeforeEach
    void setUp() {
        store = new RowStore();
    }

    private static Map<String, Object> values(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void storesAndReadsBackARow() {
        store.upsert(new Row("k1", 1L, values("a", 1)));
        assertEquals(1, store.get("k1").get("a"));
        assertEquals(1L, store.get("k1").version());
    }

    @Test
    void aNewerWriteOfTheSameColumnsWins() {
        store.upsert(new Row("k1", 1L, values("a", 1)));
        store.upsert(new Row("k1", 2L, values("a", 2)));
        assertEquals(2, store.get("k1").get("a"));
        assertEquals(2L, store.get("k1").version());
    }

    @Test
    void deleteRemovesTheRow() {
        store.upsert(new Row("k1", 1L, values("a", 1)));
        store.delete("k1");
        assertNull(store.get("k1"));
        assertFalse(store.contains("k1"));
    }

    @Test
    void sizeCountsDistinctKeys() {
        store.upsert(new Row("k1", 1L, values("a", 1)));
        store.upsert(new Row("k2", 1L, values("a", 1)));
        store.upsert(new Row("k1", 2L, values("a", 2)));
        assertEquals(2, store.size());
    }

    @Test
    void keysComeBackSorted() {
        store.upsert(new Row("k2", 1L, values("a", 1)));
        store.upsert(new Row("k1", 1L, values("a", 1)));
        assertEquals("[k1, k2]", store.keys().toString());
    }

    @Test
    void rowValuesAreImmutable() {
        Row row = new Row("k1", 1L, values("a", 1));
        assertThrows(UnsupportedOperationException.class, () -> row.values().put("b", 2));
        store.upsert(row);
        assertTrue(store.contains("k1"));
    }
}
