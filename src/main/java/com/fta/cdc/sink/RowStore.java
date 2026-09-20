package com.fta.cdc.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** In memory stand in for the destination table. */
public class RowStore {

    private final ConcurrentMap<String, Row> rows = new ConcurrentHashMap<>();

    /** Writes the row, replacing whatever is currently stored under the same key. */
    public void upsert(Row row) {
        rows.put(row.primaryKey(), row);
    }

    public void delete(String primaryKey) {
        rows.remove(primaryKey);
    }

    public Row get(String primaryKey) {
        return rows.get(primaryKey);
    }

    public boolean contains(String primaryKey) {
        return rows.containsKey(primaryKey);
    }

    public int size() {
        return rows.size();
    }

    public List<String> keys() {
        List<String> out = new ArrayList<>(rows.keySet());
        out.sort(String::compareTo);
        return out;
    }
}
