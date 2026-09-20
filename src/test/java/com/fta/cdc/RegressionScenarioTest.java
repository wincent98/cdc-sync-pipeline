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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the three robustness scenarios: schema evolution
 * (add/drop/widen/narrow), at-least-once crash recovery without duplicate writes and
 * version ordered merging of out of order events.
 */
class RegressionScenarioTest {

    private static TableSchema schema(String table, long version, ColumnDef... defs) {
        Map<String, ColumnDef> cols = new LinkedHashMap<>();
        for (ColumnDef def : defs) {
            cols.put(def.name(), def);
        }
        return new TableSchema(table, version, cols);
    }

    private static ColumnDef col(String name, ColumnType type) {
        return new ColumnDef(name, type);
    }

    private static Map<String, Object> cols(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static ChangeEvent ev(long offset, String table, String pk, Op op, long version,
                                  Map<String, Object> columns) {
        return new ChangeEvent(offset, table, pk, op, version, columns);
    }

    @Test
    void evolvesTheSchemaAcrossAddDropWidenAndNarrow() {
        SchemaRegistry registry = new SchemaRegistry();
        RowStore rows = new RowStore();
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);

        registry.register(schema("orders", 1L,
                col("id", ColumnType.intType()),
                col("note", ColumnType.varchar(8))));
        pipeline.consume(ev(0L, "orders", "O-1", Op.INSERT, 1L,
                cols("id", 1, "note", "hello")));

        // Added column lands automatically on the destination.
        registry.register(schema("orders", 2L,
                col("id", ColumnType.intType()),
                col("note", ColumnType.varchar(8)),
                col("currency", ColumnType.varchar(8))));
        pipeline.consume(ev(1L, "orders", "O-1", Op.UPDATE, 2L,
                cols("id", 1, "note", "hello", "currency", "USD")));
        assertEquals("USD", rows.get("O-1").get("currency"));

        // Dropped column keeps its last historical value; widening applies silently.
        registry.register(schema("orders", 3L,
                col("id", ColumnType.intType()),
                col("currency", ColumnType.varchar(16))));
        pipeline.consume(ev(2L, "orders", "O-1", Op.UPDATE, 3L,
                cols("id", 1, "currency", "EURO-WIDE-16CH")));
        Row row = rows.get("O-1");
        assertEquals(3L, row.version());
        assertEquals("hello", row.get("note"), "dropped column must retain last value");
        assertEquals("EURO-WIDE-16CH", row.get("currency"));

        // Narrowing sends the offending event to the DLQ and later events still flow.
        registry.register(schema("orders", 4L,
                col("id", ColumnType.intType()),
                col("currency", ColumnType.varchar(4))));
        ChangeEvent tooWide = ev(3L, "orders", "O-2", Op.INSERT, 1L,
                cols("id", 2, "currency", "USDOLLAR"));
        pipeline.consume(tooWide);
        assertEquals(1, dlq.size());
        assertEquals(tooWide, dlq.letters().get(0).event());
        assertTrue(dlq.letters().get(0).reason().contains("narrowing"));
        assertNull(rows.get("O-2"));

        pipeline.consume(ev(4L, "orders", "O-2", Op.INSERT, 2L, cols("id", 2, "currency", "EUR")));
        assertEquals("EUR", rows.get("O-2").get("currency"));
        assertEquals(1, dlq.size());
    }

    @Test
    void widensNumericColumnsAndRejectsBigintToInt() {
        SchemaRegistry registry = new SchemaRegistry();
        RowStore rows = new RowStore();
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);

        registry.register(schema("items", 1L, col("id", ColumnType.intType())));
        registry.register(schema("items", 2L, col("id", ColumnType.bigintType())));
        pipeline.consume(ev(0L, "items", "I-1", Op.INSERT, 1L, cols("id", 5_000_000_000L)));
        assertEquals(5_000_000_000L, rows.get("I-1").get("id"));

        registry.register(schema("items", 3L, col("id", ColumnType.intType())));
        ChangeEvent overflow = ev(1L, "items", "I-2", Op.INSERT, 1L, cols("id", 5_000_000_000L));
        pipeline.consume(overflow);
        assertEquals(1, dlq.size());
        assertNull(rows.get("I-2"));
        assertEquals(1L, offsets.committed());
    }

    @Test
    void recoversAfterACrashWithoutLosingOrDuplicatingRows() {
        SchemaRegistry registry = new SchemaRegistry();
        AtomicInteger upserts = new AtomicInteger();
        RowStore rows = new RowStore() {
            @Override
            public void upsert(Row row) {
                upserts.incrementAndGet();
                super.upsert(row);
                if ("O-4".equals(row.primaryKey()) && upserts.get() == 1) {
                    throw new RuntimeException("simulated crash before the offset commit");
                }
            }
        };
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);

        registry.register(schema("orders", 1L, col("id", ColumnType.intType())));
        ChangeEvent event = ev(5L, "orders", "O-4", Op.INSERT, 1L, cols("id", 4));

        // Crash happens after the write but before the offset is committed.
        try {
            pipeline.consume(event);
        } catch (RuntimeException expected) {
            assertEquals("simulated crash before the offset commit", expected.getMessage());
        }
        assertEquals(-1L, offsets.committed(), "offset must only be committed after the write");
        assertEquals(1, rows.size(), "the row must already be durable despite the crash");

        // Restart: replay from committed() + 1 must not write the durable row a second time.
        pipeline.consume(event);
        assertEquals(5L, offsets.committed());
        assertEquals(1, rows.size());
        assertEquals(1L, rows.get("O-4").version());
        assertEquals(1, upserts.get(), "the replayed event must not upsert again");
    }

    @Test
    void mergesOutOfOrderEventsByVersion() {
        SchemaRegistry registry = new SchemaRegistry();
        RowStore rows = new RowStore();
        OffsetStore offsets = new OffsetStore();
        DeadLetterQueue dlq = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rows, offsets, dlq);

        registry.register(schema("orders", 1L, col("id", ColumnType.intType())));
        pipeline.consume(ev(0L, "orders", "O-1", Op.INSERT, 1L, cols("id", 1)));
        pipeline.consume(ev(1L, "orders", "O-1", Op.UPDATE, 3L, cols("id", 30)));
        // Late, older event must be discarded even though it arrives later.
        pipeline.consume(ev(2L, "orders", "O-1", Op.UPDATE, 2L, cols("id", 20)));

        Row row = rows.get("O-1");
        assertEquals(3L, row.version());
        assertEquals(30, row.get("id"));

        // A stale DELETE must not resurrect-then-remove the newer row.
        pipeline.consume(ev(3L, "orders", "O-1", Op.DELETE, 2L, new LinkedHashMap<>()));
        assertEquals(3L, rows.get("O-1").version());

        // The stale offsets are still acknowledged so consumption keeps advancing.
        assertEquals(3L, offsets.committed());
        assertEquals(0, dlq.size());
    }

    @Test
    void keepsDifferentPrimaryKeysConsumingConcurrently() {
        SchemaRegistry registry = new SchemaRegistry();
        CountDownLatch firstInsideStore = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondProceeded = new CountDownLatch(1);
        RowStore rows = new RowStore() {
            @Override
            public void upsert(Row row) {
                if ("O-1".equals(row.primaryKey())) {
                    firstInsideStore.countDown();
                    try {
                        releaseFirst.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else if ("O-2".equals(row.primaryKey())) {
                    secondProceeded.countDown();
                }
                super.upsert(row);
            }
        };
        SyncPipeline pipeline = new SyncPipeline(registry, rows, new OffsetStore(),
                new DeadLetterQueue());
        registry.register(schema("orders", 1L, col("id", ColumnType.intType())));

        Thread first = new Thread(() -> pipeline
                .consume(ev(0L, "orders", "O-1", Op.INSERT, 1L, cols("id", 1))));
        Thread second = new Thread(() -> pipeline
                .consume(ev(1L, "orders", "O-2", Op.INSERT, 1L, cols("id", 2))));
        first.start();
        try {
            assertTrue(firstInsideStore.await(5, TimeUnit.SECONDS));
            second.start();
            assertTrue(secondProceeded.await(5, TimeUnit.SECONDS),
                    "a different primary key must not be blocked by the in-flight key");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            releaseFirst.countDown();
        }
        try {
            first.join(5000);
            second.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(2, rows.size());
    }
}
