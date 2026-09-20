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
import com.fta.cdc.sink.Row;
import com.fta.cdc.sink.RowStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for schema evolution, crash-safe offset commits and out of
 * order version merging.
 */
class RegressionTest {

    private static TableSchema schema(String table, long version, Object... kv) {
        Map<String, ColumnDef> cols = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            cols.put((String) kv[i], new ColumnDef((String) kv[i], (ColumnType) kv[i + 1]));
        }
        return new TableSchema(table, version, cols);
    }

    private static Map<String, Object> cols(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static ChangeEvent ev(long offset, String table, String pk, Op op, long version,
                                  Map<String, Object> c) {
        return new ChangeEvent(offset, table, pk, op, version, c);
    }

    @Test
    void evolvesAcrossAddedDroppedWidenedAndNarrowedColumns() {
        SchemaRegistry registry = new SchemaRegistry();
        RowStore rows = new RowStore();
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);

        registry.register(schema("orders", 1, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType(), "note", ColumnType.varchar(32)));
        pipeline.consume(ev(0, "orders", "O-1", Op.INSERT, 1,
                cols("id", 1, "amount", 100L, "note", "a")));

        // DDL: added column lands automatically, even though the table view was cached.
        registry.register(schema("orders", 2, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType(), "note", ColumnType.varchar(32),
                "currency", ColumnType.varchar(8)));
        pipeline.consume(ev(1, "orders", "O-2", Op.INSERT, 1,
                cols("id", 2, "amount", 200L, "note", "b", "currency", "EUR")));
        assertEquals("EUR", rows.get("O-2").get("currency"));
        assertEquals(2L, registry.cached("orders").schemaVersion());

        // DDL: note dropped upstream; its last historical value must survive on O-1.
        registry.register(schema("orders", 3, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType(), "currency", ColumnType.varchar(8)));
        pipeline.consume(ev(2, "orders", "O-1", Op.UPDATE, 2,
                cols("id", 1, "amount", 180L, "currency", "USD")));
        Row o1 = rows.get("O-1");
        assertEquals("a", o1.get("note"));
        assertEquals("USD", o1.get("currency"));

        // A DDL on another table bumps the shared generation; the orders view must stay valid.
        registry.register(schema("items", 1, "sku", ColumnType.intType()));
        pipeline.consume(ev(3, "orders", "O-2", Op.UPDATE, 2,
                cols("id", 2, "amount", 210L, "currency", "EUR")));
        assertEquals("EUR", rows.get("O-2").get("currency"));

        // VARCHAR widening is silently compatible.
        registry.register(schema("orders", 4, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType(), "currency", ColumnType.varchar(32)));
        pipeline.consume(ev(4, "orders", "O-2", Op.UPDATE, 3,
                cols("id", 2, "amount", 260L, "currency", "EUR-WIDE-16CH")));
        assertEquals("EUR-WIDE-16CH", rows.get("O-2").get("currency"));

        // VARCHAR narrowing: the carrying event is dead lettered, nothing is partially applied.
        registry.register(schema("orders", 5, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType(), "currency", ColumnType.varchar(4)));
        pipeline.consume(ev(5, "orders", "O-1", Op.UPDATE, 3,
                cols("id", 1, "amount", 999L, "currency", "USDOLLAR")));
        assertEquals(1, dlq.size());
        assertTrue(dlq.letters().get(0).reason().contains("currency"));
        assertEquals(180L, rows.get("O-1").get("amount"));
        assertEquals("USD", rows.get("O-1").get("currency"));

        // The pipeline keeps consuming and committing after a dead letter.
        pipeline.consume(ev(6, "orders", "O-3", Op.INSERT, 1,
                cols("id", 3, "amount", 300L)));
        assertEquals(6L, offsets.committed());
        assertEquals(300L, rows.get("O-3").get("amount"));

        // INT -> BIGINT widening coerces the decoded value; BIGINT -> INT then dead letters.
        registry.register(schema("items", 2, "sku", ColumnType.bigintType()));
        pipeline.consume(ev(7, "items", "I-1", Op.INSERT, 1, cols("sku", 7)));
        assertEquals(Long.valueOf(7L), rows.get("I-1").get("sku"));
        registry.register(schema("items", 3, "sku", ColumnType.intType()));
        pipeline.consume(ev(8, "items", "I-1", Op.UPDATE, 2, cols("sku", 8)));
        assertEquals(2, dlq.size());
        assertTrue(dlq.letters().get(1).reason().contains("sku"));
        assertEquals(Long.valueOf(7L), rows.get("I-1").get("sku"));
    }

    @Test
    void commitsOnlyAfterTheRowLandsAndResumesFromTheCommittedOffset() {
        SchemaRegistry registry = new SchemaRegistry();
        OnceFailingRowStore rows = new OnceFailingRowStore("O-2");
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);
        registry.register(schema("orders", 1, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType()));

        pipeline.consume(ev(4, "orders", "O-1", Op.INSERT, 1, cols("id", 1, "amount", 100L)));

        // Simulated crash while writing O-2: the offset must not move ahead of the store.
        assertThrows(RuntimeException.class,
                () -> pipeline.consume(ev(5, "orders", "O-2", Op.INSERT, 1,
                        cols("id", 2, "amount", 200L))));
        assertEquals(4L, offsets.committed());
        assertNull(rows.get("O-2"));
        assertEquals(1, rows.completed("O-1"));

        // Restart replays from committed() + 1: the row lands once and the commit then follows.
        pipeline.consume(ev(5, "orders", "O-2", Op.INSERT, 1, cols("id", 2, "amount", 200L)));
        assertEquals(5L, offsets.committed());
        assertEquals(200L, rows.get("O-2").get("amount"));
        assertEquals(1, rows.completed("O-2"));
    }

    @Test
    void mergesOutOfOrderEventsByVersionUnderPrimaryKeyConcurrency() throws Exception {
        SchemaRegistry registry = new SchemaRegistry();
        RowStore rows = new RowStore();
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);
        registry.register(schema("orders", 1, "id", ColumnType.intType(),
                "amount", ColumnType.bigintType()));

        pipeline.consume(ev(0, "orders", "O-1", Op.INSERT, 1, cols("id", 1, "amount", 100L)));
        pipeline.consume(ev(1, "orders", "O-1", Op.UPDATE, 3, cols("id", 1, "amount", 300L)));

        // Older version: discarded, still advances the offset so it is never replayed.
        pipeline.consume(ev(2, "orders", "O-1", Op.UPDATE, 2, cols("id", 1, "amount", 250L)));
        assertEquals(300L, rows.get("O-1").get("amount"));
        assertEquals(3L, rows.get("O-1").version());
        assertEquals(2L, offsets.committed());

        // A stale delete cannot remove a row that is already newer.
        pipeline.consume(ev(3, "orders", "O-1", Op.DELETE, 2, cols()));
        assertEquals(300L, rows.get("O-1").get("amount"));

        // Concurrent shuffled delivery of many versions per key converges to the newest.
        int keys = 8;
        int versions = 25;
        List<ChangeEvent> batch = new ArrayList<>();
        long offset = 10;
        for (int k = 0; k < keys; k++) {
            for (int v = 1; v <= versions; v++) {
                batch.add(ev(offset++, "orders", "K-" + k, Op.UPDATE, v,
                        cols("id", k, "amount", (long) v)));
            }
        }
        Collections.shuffle(batch, new Random(42));

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch done = new CountDownLatch(batch.size());
        for (ChangeEvent event : batch) {
            pool.submit(() -> {
                try {
                    pipeline.consume(event);
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdownNow();

        for (int k = 0; k < keys; k++) {
            Row row = rows.get("K-" + k);
            assertEquals(versions, row.version());
            assertEquals((long) versions, row.get("amount"));
        }
    }

    /** Fails the first upsert of one key, like a process crash mid write. */
    private static final class OnceFailingRowStore extends RowStore {
        private final String crashKey;
        private final Set<String> failed = ConcurrentHashMap.newKeySet();
        private final Map<String, Integer> completed = new ConcurrentHashMap<>();

        private OnceFailingRowStore(String crashKey) {
            this.crashKey = crashKey;
        }

        @Override
        public void upsert(Row row) {
            if (crashKey.equals(row.primaryKey()) && failed.add(row.primaryKey())) {
                throw new RuntimeException("simulated crash while writing " + row.primaryKey());
            }
            super.upsert(row);
            completed.merge(row.primaryKey(), 1, Integer::sum);
        }

        private int completed(String key) {
            return completed.getOrDefault(key, 0);
        }
    }
}
