package com.fta.cdc;

import com.fta.cdc.event.ChangeEvent;
import com.fta.cdc.event.DeadLetterQueue;
import com.fta.cdc.event.Op;
import com.fta.cdc.offset.OffsetStore;
import com.fta.cdc.pipeline.SyncPipeline;
import com.fta.cdc.schema.ColumnDef;
import com.fta.cdc.schema.ColumnType;
import com.fta.cdc.schema.SchemaRegistry;
import com.fta.cdc.schema.TableSchema;
import com.fta.cdc.sink.RowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncPipelineTest {

    private SchemaRegistry registry;
    private RowStore rowStore;
    private OffsetStore offsetStore;
    private DeadLetterQueue deadLetters;
    private SyncPipeline pipeline;

    @BeforeEach
    void setUp() {
        registry = new SchemaRegistry();
        rowStore = new RowStore();
        offsetStore = new OffsetStore();
        deadLetters = new DeadLetterQueue();
        pipeline = new SyncPipeline(registry, rowStore, offsetStore, deadLetters);
        registry.register(orders(1L, "id", "amount"));
    }

    private static TableSchema orders(long version, String... columns) {
        Map<String, ColumnDef> cols = new LinkedHashMap<>();
        for (String c : columns) {
            cols.put(c, new ColumnDef(c, "id".equals(c) ? ColumnType.intType() : ColumnType.bigintType()));
        }
        return new TableSchema("orders", version, cols);
    }

    private static Map<String, Object> cols(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static ChangeEvent ev(long offset, String pk, Op op, long version, Map<String, Object> c) {
        return new ChangeEvent(offset, "orders", pk, op, version, c);
    }

    @Test
    void appliesAnInsert() {
        pipeline.consume(ev(0L, "O-1", Op.INSERT, 1L, cols("id", 1, "amount", 100L)));
        assertEquals(1, rowStore.get("O-1").get("id"));
        assertEquals(100L, rowStore.get("O-1").get("amount"));
    }

    @Test
    void aNewerUpdateReplacesTheValue() {
        pipeline.consume(ev(0L, "O-1", Op.INSERT, 1L, cols("id", 1, "amount", 100L)));
        pipeline.consume(ev(1L, "O-1", Op.UPDATE, 2L, cols("id", 1, "amount", 150L)));
        assertEquals(150L, rowStore.get("O-1").get("amount"));
        assertEquals(2L, rowStore.get("O-1").version());
    }

    @Test
    void appliesADelete() {
        pipeline.consume(ev(0L, "O-1", Op.INSERT, 1L, cols("id", 1, "amount", 100L)));
        pipeline.consume(ev(1L, "O-1", Op.DELETE, 2L, cols()));
        assertNull(rowStore.get("O-1"));
    }

    @Test
    void commitsTheOffsetOfEveryAppliedEvent() {
        pipeline.consume(ev(4L, "O-1", Op.INSERT, 1L, cols("id", 1, "amount", 100L)));
        assertEquals(4L, offsetStore.committed());
    }

    @Test
    void deadLettersEventsOfUnknownTables() {
        ChangeEvent foreign = new ChangeEvent(2L, "shipments", "S-1", Op.INSERT, 1L, cols("id", 1));
        pipeline.consume(foreign);
        assertEquals(1, deadLetters.size());
        assertFalse(rowStore.contains("S-1"));
        assertEquals("shipments", deadLetters.letters().get(0).event().table());
    }

    @Test
    void keepsDistinctPrimaryKeysApart() {
        pipeline.consume(ev(0L, "O-1", Op.INSERT, 1L, cols("id", 1, "amount", 100L)));
        pipeline.consume(ev(1L, "O-2", Op.INSERT, 1L, cols("id", 2, "amount", 200L)));
        assertEquals(2, rowStore.size());
        assertTrue(rowStore.contains("O-1"));
        assertTrue(rowStore.contains("O-2"));
    }
}
