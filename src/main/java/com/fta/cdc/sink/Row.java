package com.fta.cdc.sink;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class Row {

    private final String primaryKey;
    private final long version;
    private final Map<String, Object> values;

    public Row(String primaryKey, long version, Map<String, Object> values) {
        this.primaryKey = primaryKey;
        this.version = version;
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public String primaryKey() {
        return primaryKey;
    }

    public long version() {
        return version;
    }

    public Map<String, Object> values() {
        return values;
    }

    public Object get(String column) {
        return values.get(column);
    }

    @Override
    public String toString() {
        return primaryKey + "(v" + version + ")" + values;
    }
}
