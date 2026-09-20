package com.fta.cdc.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** One version of a table definition. */
public final class TableSchema {

    private final String table;
    private final long schemaVersion;
    private final Map<String, ColumnDef> columns;

    public TableSchema(String table, long schemaVersion, Map<String, ColumnDef> columns) {
        this.table = table;
        this.schemaVersion = schemaVersion;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    public String table() {
        return table;
    }

    public long schemaVersion() {
        return schemaVersion;
    }

    public Map<String, ColumnDef> columns() {
        return columns;
    }

    public Set<String> columnNames() {
        return columns.keySet();
    }

    public boolean hasColumn(String name) {
        return columns.containsKey(name);
    }

    public ColumnDef column(String name) {
        return columns.get(name);
    }

    @Override
    public String toString() {
        return table + "@v" + schemaVersion + columns.keySet();
    }
}
