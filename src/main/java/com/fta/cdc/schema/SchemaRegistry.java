package com.fta.cdc.schema;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds the current definition of every table the pipeline syncs.
 *
 * The pipeline reads the cached definition on every event, so the cache must not be
 * visible in a half updated state to a consumer that is committing an offset.
 */
public class SchemaRegistry {

    private final ConcurrentMap<String, TableSchema> current = new ConcurrentHashMap<>();
    private volatile long generation;

    public void register(TableSchema schema) {
        current.put(schema.table(), schema);
        generation++;
    }

    public TableSchema current(String table) {
        TableSchema schema = current.get(table);
        if (schema == null) {
            throw new IllegalStateException("no schema registered for table " + table);
        }
        return schema;
    }

    public boolean knows(String table) {
        return current.containsKey(table);
    }

    public long generation() {
        return generation;
    }
}
