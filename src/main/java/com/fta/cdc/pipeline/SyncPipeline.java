package com.fta.cdc.pipeline;

import com.fta.cdc.event.ChangeEvent;
import com.fta.cdc.event.DeadLetterQueue;
import com.fta.cdc.event.Op;
import com.fta.cdc.offset.OffsetStore;
import com.fta.cdc.schema.SchemaRegistry;
import com.fta.cdc.schema.TableSchema;
import com.fta.cdc.sink.Row;
import com.fta.cdc.sink.RowStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Consumes change events and applies them to the destination table. */
public class SyncPipeline {

    private final SchemaRegistry registry;
    private final RowStore rowStore;
    private final OffsetStore offsetStore;
    private final DeadLetterQueue deadLetters;

    // Resolving the schema on every event was measurably hot, so the first lookup is cached.
    private final ConcurrentMap<String, TableSchema> schemaCache = new ConcurrentHashMap<>();

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

        offsetStore.commit(event.offset());

        TableSchema schema = schemaCache.computeIfAbsent(event.table(), registry::current);

        if (event.op() == Op.DELETE) {
            rowStore.delete(event.primaryKey());
            return;
        }

        Map<String, Object> projected = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : event.columns().entrySet()) {
            if (schema.hasColumn(entry.getKey())) {
                projected.put(entry.getKey(), entry.getValue());
            }
        }
        rowStore.upsert(new Row(event.primaryKey(), event.version(), projected));
    }

    public DeadLetterQueue deadLetters() {
        return deadLetters;
    }

    public int cachedSchemas() {
        return schemaCache.size();
    }
}
