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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Consumes change events and applies them to the destination table. */
public class SyncPipeline {

    private final SchemaRegistry registry;
    private final RowStore rowStore;
    private final OffsetStore offsetStore;
    private final DeadLetterQueue deadLetters;

    // Latest schema used to project a table. The cache is invalidated inside the same
    // per-key critical section as the offset commit (see refreshSchema), so a restart can
    // never decode a new event with a stale column mapping.
    private final ConcurrentMap<String, TableSchema> schemaCache = new ConcurrentHashMap<>();

    // Most recent schema already reconciled with the applied/dead-lettered events of a
    // table, so the event straddling a narrowing DDL can be recognised.
    private final ConcurrentMap<String, TableSchema> observedSchemas = new ConcurrentHashMap<>();

    // One lock per primary key: version gate, write and offset commit of a key are one
    // critical section, while distinct keys keep consuming concurrently.
    private final ConcurrentMap<String, Object> keyLocks = new ConcurrentHashMap<>();

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

        synchronized (lockFor(event.primaryKey())) {
            TableSchema schema = refreshSchema(event.table());

            Row stored = rowStore.get(event.primaryKey());
            if (stored != null && event.version() < stored.version()) {
                // Out of order delivery: an older event must never overwrite newer data.
                offsetStore.commit(event.offset());
                return;
            }

            if (event.op() == Op.DELETE) {
                if (stored == null || event.version() > stored.version()) {
                    rowStore.delete(event.primaryKey());
                }
                // Equal version is the at-least-once replay after a crash: nothing to redo.
                offsetStore.commit(event.offset());
                return;
            }

            Map<String, Object> projected = new LinkedHashMap<>();
            for (Map.Entry<String, Object> column : event.columns().entrySet()) {
                ColumnDef def = schema.column(column.getKey());
                if (def == null) {
                    // Column dropped upstream. The value is absent from the event and the
                    // merge below keeps the last historical value in the destination.
                    continue;
                }
                if (!def.type().accepts(column.getValue())) {
                    refuse(event, schema, "incompatible narrowing of column " + column.getKey()
                            + ": value " + column.getValue() + " does not fit " + def.type());
                    return;
                }
                projected.put(column.getKey(), column.getValue());
            }

            TableSchema previous = observedSchemas.get(event.table());
            if (previous != null && straddlesNarrowing(previous, schema, projected.keySet())) {
                refuse(event, schema, "incompatible schema change between v"
                        + previous.schemaVersion() + " and v" + schema.schemaVersion()
                        + ": a column was narrowed");
                return;
            }

            Map<String, Object> merged = new LinkedHashMap<>();
            if (stored != null) {
                // Preserve columns the DDL dropped and columns this event does not carry.
                merged.putAll(stored.values());
            }
            merged.putAll(projected);

            if (stored == null || event.version() > stored.version()) {
                rowStore.upsert(new Row(event.primaryKey(), event.version(), merged));
            }
            // Equal version is an at-least-once replay after a crash between upsert and
            // commit: the row is already durable, so skip the write and only commit.

            observedSchemas.put(event.table(), schema);
            offsetStore.commit(event.offset());
        }
    }

    public DeadLetterQueue deadLetters() {
        return deadLetters;
    }

    public int cachedSchemas() {
        return schemaCache.size();
    }

    private void refuse(ChangeEvent event, TableSchema schema, String reason) {
        deadLetters.put(event, reason);
        observedSchemas.put(event.table(), schema);
        offsetStore.commit(event.offset());
    }

    private Object lockFor(String primaryKey) {
        return keyLocks.computeIfAbsent(primaryKey, key -> new Object());
    }

    /**
     * Reconciles the cached schema with the registry before the event is projected. Cache
     * invalidation and the offset commit happen inside the same per-key critical section
     * (the caller holds the key lock when committing), so a restart can never resume with
     * an old column mapping.
     */
    private TableSchema refreshSchema(String table) {
        TableSchema latest = registry.current(table);
        TableSchema cached = schemaCache.get(table);
        if (cached == null || cached.schemaVersion() != latest.schemaVersion() || cached != latest) {
            schemaCache.put(table, latest);
        }
        return latest;
    }

    private boolean straddlesNarrowing(TableSchema previous, TableSchema current,
                                       Set<String> touchedColumns) {
        for (String name : touchedColumns) {
            ColumnDef oldDef = previous.column(name);
            ColumnDef newDef = current.column(name);
            if (oldDef == null || newDef == null) {
                continue;
            }
            if (newDef.type().evolutionFrom(oldDef.type()) == ColumnType.Evolution.INCOMPATIBLE) {
                return true;
            }
        }
        return false;
    }
}
