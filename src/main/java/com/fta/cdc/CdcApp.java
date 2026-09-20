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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Replays a fixed binlog stream that contains four DDL steps, one out of order event
 * and one simulated crash, then compares the destination table against the expected
 * end state and prints a consistency report.
 */
public final class CdcApp {

    private static final String TABLE = "orders";
    private static final String CRASH_KEY = "O-4";

    public static void main(String[] args) {
        SchemaRegistry registry = new SchemaRegistry();
        RowStore rowStore = new FlakyRowStore();
        OffsetStore offsetStore = new OffsetStore();
        DeadLetterQueue deadLetters = new DeadLetterQueue();
        SyncPipeline pipeline = new SyncPipeline(registry, rowStore, offsetStore, deadLetters);

        List<ChangeEvent> stream = stream();
        Map<Long, TableSchema> ddl = ddlTimeline();

        System.out.println("cdc-sync-pipeline 2.1.0");
        System.out.println("events=" + stream.size() + " ddl steps=" + ddl.size() + " table=" + TABLE);
        System.out.println("replaying stream ...");

        long next = 0;
        int crashes = 0;
        while (next < stream.size()) {
            TableSchema pending = ddl.get(next);
            if (pending != null) {
                registry.register(pending);
                System.out.println("  ddl  @" + next + "  -> " + pending);
            }
            try {
                pipeline.consume(stream.get((int) next));
                next++;
            } catch (RuntimeException e) {
                crashes++;
                long resume = offsetStore.committed() + 1;
                System.out.println("  CRASH @" + next + " (" + e.getMessage() + "), resuming from offset " + resume);
                next = resume;
            }
        }

        report(rowStore, deadLetters, crashes);
    }

    private static Map<Long, TableSchema> ddlTimeline() {
        Map<Long, TableSchema> ddl = new TreeMap<>();
        ddl.put(0L, schema(1, col("id", ColumnType.intType()), col("amount", ColumnType.bigintType()),
                col("note", ColumnType.varchar(32))));
        ddl.put(3L, schema(2, col("id", ColumnType.intType()), col("amount", ColumnType.bigintType()),
                col("note", ColumnType.varchar(32)), col("currency", ColumnType.varchar(8))));
        ddl.put(6L, schema(3, col("id", ColumnType.intType()), col("amount", ColumnType.bigintType()),
                col("currency", ColumnType.varchar(8))));
        ddl.put(8L, schema(4, col("id", ColumnType.intType()), col("amount", ColumnType.bigintType()),
                col("currency", ColumnType.varchar(16))));
        ddl.put(10L, schema(5, col("id", ColumnType.intType()), col("amount", ColumnType.bigintType()),
                col("currency", ColumnType.varchar(4))));
        return ddl;
    }

    private static List<ChangeEvent> stream() {
        List<ChangeEvent> out = new ArrayList<>();
        out.add(ev(0, "O-1", Op.INSERT, 1, cols("id", 1, "amount", 100L, "note", "a")));
        out.add(ev(1, "O-2", Op.INSERT, 1, cols("id", 2, "amount", 200L, "note", "b")));
        out.add(ev(2, "O-1", Op.UPDATE, 2, cols("id", 1, "amount", 150L, "note", "a2")));
        out.add(ev(3, "O-3", Op.INSERT, 1, cols("id", 3, "amount", 300L, "note", "c", "currency", "USD")));
        out.add(ev(4, "O-2", Op.UPDATE, 2, cols("id", 2, "amount", 250L, "note", "b2", "currency", "EUR")));
        out.add(ev(5, "O-4", Op.INSERT, 1, cols("id", 4, "amount", 400L, "note", "d", "currency", "JPY")));
        out.add(ev(6, "O-1", Op.UPDATE, 3, cols("id", 1, "amount", 180L, "currency", "USD")));
        out.add(ev(7, "O-5", Op.INSERT, 1, cols("id", 5, "amount", 500L, "currency", "GBP")));
        out.add(ev(8, "O-2", Op.UPDATE, 3, cols("id", 2, "amount", 260L, "currency", "EUR-WIDE-16CH")));
        out.add(ev(9, "O-1", Op.UPDATE, 2, cols("id", 1, "amount", 999L, "currency", "XXX")));
        out.add(ev(10, "O-6", Op.INSERT, 1, cols("id", 6, "amount", 600L, "currency", "USDOLLAR")));
        return out;
    }

    /** End state the destination table must reach. O-6 is left out on purpose. */
    private static Map<String, Expected> expected() {
        Map<String, Expected> out = new LinkedHashMap<>();
        out.put("O-1", new Expected(3, cols("id", 1, "amount", 180L, "note", "a2", "currency", "USD")));
        out.put("O-2", new Expected(3, cols("id", 2, "amount", 260L, "note", "b2", "currency", "EUR-WIDE-16CH")));
        out.put("O-3", new Expected(1, cols("id", 3, "amount", 300L, "note", "c", "currency", "USD")));
        out.put("O-4", new Expected(1, cols("id", 4, "amount", 400L, "note", "d", "currency", "JPY")));
        out.put("O-5", new Expected(1, cols("id", 5, "amount", 500L, "currency", "GBP")));
        return out;
    }

    private static void report(RowStore store, DeadLetterQueue deadLetters, int crashes) {
        Map<String, Expected> expected = expected();
        int drift = 0;
        int lost = 0;
        int stale = 0;

        System.out.println();
        System.out.println("=== consistency report ===");
        for (Map.Entry<String, Expected> entry : expected.entrySet()) {
            String pk = entry.getKey();
            Expected want = entry.getValue();
            Row row = store.get(pk);
            if (row == null) {
                lost++;
                System.out.println("  " + pk + "  MISSING");
                continue;
            }
            if (row.version() < want.version) {
                stale++;
                System.out.println("  " + pk + "  STALE  stored v" + row.version() + " expected v" + want.version);
            }
            for (Map.Entry<String, Object> c : want.values.entrySet()) {
                if (!Objects.equals(row.get(c.getKey()), c.getValue())) {
                    drift++;
                    System.out.println("  " + pk + "  DRIFT  " + c.getKey()
                            + " stored=" + row.get(c.getKey()) + " expected=" + c.getValue());
                }
            }
        }

        int extra = 0;
        for (String key : store.keys()) {
            if (!expected.containsKey(key)) {
                extra++;
            }
        }

        System.out.println();
        System.out.println("simulated crashes : " + crashes);
        System.out.println("schema drift      : " + drift);
        System.out.println("lost rows         : " + lost);
        System.out.println("stale overwrite   : " + stale);
        System.out.println("dead letters      : " + deadLetters.size());
        System.out.println("unexpected rows   : " + extra);

        if (drift + lost + stale > 0) {
            System.out.println();
            System.out.println("FAILED: destination table does not match the expected end state");
            System.exit(1);
        }
        System.out.println();
        System.out.println("OK: destination table matches the expected end state");
    }

    private static final class Expected {
        private final long version;
        private final Map<String, Object> values;

        private Expected(long version, Map<String, Object> values) {
            this.version = version;
            this.values = values;
        }
    }

    /** Fails the very first write of one key to emulate a process crash mid batch. */
    private static final class FlakyRowStore extends RowStore {
        private boolean armed = true;

        @Override
        public void upsert(Row row) {
            if (armed && CRASH_KEY.equals(row.primaryKey())) {
                armed = false;
                throw new RuntimeException("simulated crash while writing " + row.primaryKey());
            }
            super.upsert(row);
        }
    }

    private static ColumnDef col(String name, ColumnType type) {
        return new ColumnDef(name, type);
    }

    private static TableSchema schema(long version, ColumnDef... defs) {
        Map<String, ColumnDef> cols = new LinkedHashMap<>();
        for (ColumnDef def : defs) {
            cols.put(def.name(), def);
        }
        return new TableSchema(TABLE, version, cols);
    }

    private static ChangeEvent ev(long offset, String pk, Op op, long version, Map<String, Object> cols) {
        return new ChangeEvent(offset, TABLE, pk, op, version, cols);
    }

    private static Map<String, Object> cols(Object... kv) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            out.put((String) kv[i], kv[i + 1]);
        }
        return out;
    }

    private CdcApp() {
    }
}
