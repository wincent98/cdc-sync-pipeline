package com.fta.cdc.schema;

public final class ColumnDef {

    private final String name;
    private final ColumnType type;

    public ColumnDef(String name, ColumnType type) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("column name must not be empty");
        }
        this.name = name;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public ColumnType type() {
        return type;
    }

    @Override
    public String toString() {
        return name + " " + type;
    }
}
