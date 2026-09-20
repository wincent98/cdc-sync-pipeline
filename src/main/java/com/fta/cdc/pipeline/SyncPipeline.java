package com.fta.cdc.pipeline;

import com.fta.cdc.event.ChangeEvent;
import com.fta.cdc.event.DeadLetterQueue;
import com.fta.cdc.event.Op;
import com.fta.cdc.offset.OffsetStore;
import com.fta.cdc.schema.ColumnDef;
import com.fta.cdc.schema.ColumnType;
import com.fta.cdc.schema.SchemaRegistry;
import com.fta.cdc.schema.TableSchema;
import com.fta.cdc.sink.Row;
import com.fta.cdc.sink.RowStore;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Consumes change events and applies them to the destination table. */
public class SyncPipeline {

    /** Concurrency stays at primary-key granularity. */
    private static final int LOCK_STRIPES = 16;

    private final SchemaRegistry registry;
    private final RowStore rowStore;
    private final OffsetStore offsetStore;
    private final DeadLetterQueue deadLetters;

    private final Object[] stripes = createStripes();
    // Per table evolving view of the target columns (widened types are sticky,
    // narrowed columns are remembered until the upstream widens them again).
    private final ConcurrentMap<String, TableView> views = new ConcurrentHashMap<>();

    public SyncPipeline(SchemaRegistry registry, RowStore rowStore, OffsetStore offsetStore,
                        DeadLetterQueue deadLetters) {
        this.registry = registry;
        this.rowStore = rowStore;
        this.offsetStore = offsetStore;
        this.deadLetters = deadLetters;
    }

    public void consume(ChangeEvent event) {
        if (!registry.knows(event.table())) {
            deadLetters.put(event, "unknown table " + event.table());
            offsetStore.commit(event.offset());
            return;
        }

        synchronized (stripeFor(event.table(), event.primaryKey())) {
            // Schema cache invalidation, the row write and the offset commit share one
            // primary-key critical section, so a restart never pairs a new offset with a
            // stale column mapping and never replays a row that already landed.
            TableView view = refreshView(event.table());

            Row existing = rowStore.get(event.primaryKey());
            if (existing != null && event.version() < existing.version()) {
                // Out of order delivery: the destination already holds a newer version.
                offsetStore.commit(event.offset());
                return;
            }

            if (event.op() == Op.DELETE) {
                rowStore.delete(event.primaryKey());
                offsetStore.commit(event.offset());
                return;
            }

            for (String column : event.columns().keySet()) {
                if (view.incompatible.contains(column)) {
                    deadLetters.put(event, "incompatible schema change on column "
                            + column + " (type narrowing)");
                    offsetStore.commit(event.offset());
                    return;
                }
            }

            // Project onto the target table. Columns dropped upstream are absent from the
            // event and survive here as their last historical value.
            Map<String, Object> merged = new LinkedHashMap<>();
            if (existing != null) {
                merged.putAll(existing.values());
            }
            for (Map.Entry<String, Object> entry : event.columns().entrySet()) {
                if (!view.schema.hasColumn(entry.getKey())) {
                    continue;
                }
                ColumnType effectiveType = view.effectiveTypes.get(entry.getKey());
                merged.put(entry.getKey(),
                        effectiveType != null ? effectiveType.coerce(entry.getValue()) : entry.getValue());
            }

            rowStore.upsert(new Row(event.primaryKey(), event.version(), merged));
            offsetStore.commit(event.offset());
        }
    }

    public DeadLetterQueue deadLetters() {
        return deadLetters;
    }

    public int cachedSchemas() {
        return views.size();
    }

    private static Object[] createStripes() {
        Object[] locks = new Object[LOCK_STRIPES];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
        return locks;
    }

    private Object stripeFor(String table, String primaryKey) {
        int hash = (table + '\u0000' + primaryKey).hashCode() & Integer.MAX_VALUE;
        return stripes[hash % LOCK_STRIPES];
    }

    /**
     * Invalidates the registry cache inside the caller's critical section whenever a new
     * DDL generation is visible and rebuilds the evolving view of the table.
     */
    private TableView refreshView(String table) {
        long generation = registry.generation();
        TableView view = views.get(table);
        if (view != null && generation == view.generation) {
            return view;
        }
        registry.invalidate(table);
        TableSchema schema = registry.resolve(table);

        Map<String, ColumnType> effectiveTypes;
        Set<String> incompatible;
        if (view == null) {
            effectiveTypes = new LinkedHashMap<>();
            incompatible = new LinkedHashSet<>();
            for (Map.Entry<String, ColumnDef> entry : schema.columns().entrySet()) {
                effectiveTypes.put(entry.getKey(), entry.getValue().type());
            }
        } else {
            effectiveTypes = new LinkedHashMap<>(view.effectiveTypes);
            incompatible = new LinkedHashSet<>(view.incompatible);
            for (Map.Entry<String, ColumnDef> entry : schema.columns().entrySet()) {
                String name = entry.getKey();
                ColumnType upstream = entry.getValue().type();
                ColumnType previous = effectiveTypes.get(name);
                if (previous == null || upstream.isWideningOf(previous)) {
                    // Added column, unchanged column or a silent widening.
                    effectiveTypes.put(name, upstream);
                    incompatible.remove(name);
                } else {
                    // Narrowing: keep the last compatible (wider) target type and mark the
                    // column so carrying events are dead lettered instead of applied.
                    incompatible.add(name);
                }
            }
        }
        view = new TableView(schema, generation, effectiveTypes, incompatible);
        views.put(table, view);
        return view;
    }

    private static final class TableView {
        private final long generation;
        private final TableSchema schema;
        private final Map<String, ColumnType> effectiveTypes;
        private final Set<String> incompatible;

        private TableView(TableSchema schema, long generation, Map<String, ColumnType> effectiveTypes,
                          Set<String> incompatible) {
            this.schema = schema;
            this.generation = generation;
            this.effectiveTypes = effectiveTypes;
            this.incompatible = incompatible;
        }
    }
}
