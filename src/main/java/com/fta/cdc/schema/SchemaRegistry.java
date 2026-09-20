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
    private final ConcurrentMap<String, TableSchema> cache = new ConcurrentHashMap<>();
    private volatile long generation;

    public void register(TableSchema schema) {
        current.put(schema.table(), schema);
        generation++;
    }

    /**
     * Resolves the registered schema through the cached view. Callers that need cache
     * invalidation and offset commit to be atomic must call {@link #invalidate(String)}
     * and this method from inside that critical section.
     */
    public TableSchema resolve(String table) {
        TableSchema schema = cache.get(table);
        if (schema == null) {
            schema = current(table);
            cache.putIfAbsent(table, schema);
        }
        return schema;
    }

    /**
     * Drops the cached definition of one table so the next resolve reads the freshly
     * registered DDL. Safe to call when the table is not cached.
     */
    public void invalidate(String table) {
        cache.remove(table);
    }

    public TableSchema current(String table) {
        TableSchema schema = current.get(table);
        if (schema == null) {
            throw new IllegalStateException("no schema registered for table " + table);
        }
        return schema;
    }

    public TableSchema cached(String table) {
        return cache.get(table);
    }

    public boolean knows(String table) {
        return current.containsKey(table);
    }

    public long generation() {
        return generation;
    }
}
