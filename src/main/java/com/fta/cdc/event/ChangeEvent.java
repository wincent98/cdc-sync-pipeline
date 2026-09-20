package com.fta.cdc.event;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One row change decoded from the upstream binlog stream. */
public final class ChangeEvent {

    private final long offset;
    private final String table;
    private final String primaryKey;
    private final Op op;
    private final long version;
    private final Map<String, Object> columns;

    public ChangeEvent(long offset, String table, String primaryKey, Op op, long version,
                       Map<String, Object> columns) {
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table must not be empty");
        }
        if (primaryKey == null || primaryKey.isEmpty()) {
            throw new IllegalArgumentException("primaryKey must not be empty");
        }
        this.offset = offset;
        this.table = table;
        this.primaryKey = primaryKey;
        this.op = op;
        this.version = version;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns == null ? new LinkedHashMap<>() : columns));
    }

    public long offset() {
        return offset;
    }

    public String table() {
        return table;
    }

    public String primaryKey() {
        return primaryKey;
    }

    public Op op() {
        return op;
    }

    public long version() {
        return version;
    }

    public Map<String, Object> columns() {
        return columns;
    }

    @Override
    public String toString() {
        return "ChangeEvent{off=" + offset + ", " + table + "#" + primaryKey + ", " + op + ", v" + version + ", " + columns + '}';
    }
}
