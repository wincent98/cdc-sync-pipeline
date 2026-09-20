package com.fta.cdc.schema;

import java.util.Objects;

/** Column type of the upstream table. Only the subset the pipeline has to understand. */
public final class ColumnType {

    public enum Kind {
        INT,
        BIGINT,
        VARCHAR
    }

    private final Kind kind;
    private final int length;

    private ColumnType(Kind kind, int length) {
        this.kind = kind;
        this.length = length;
    }

    public static ColumnType intType() {
        return new ColumnType(Kind.INT, 0);
    }

    public static ColumnType bigintType() {
        return new ColumnType(Kind.BIGINT, 0);
    }

    public static ColumnType varchar(int length) {
        if (length <= 0) {
            throw new IllegalArgumentException("varchar length must be positive");
        }
        return new ColumnType(Kind.VARCHAR, length);
    }

    public Kind kind() {
        return kind;
    }

    public int length() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ColumnType)) {
            return false;
        }
        ColumnType other = (ColumnType) o;
        return kind == other.kind && length == other.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, length);
    }

    @Override
    public String toString() {
        return kind == Kind.VARCHAR ? "VARCHAR(" + length + ")" : kind.name();
    }
}
