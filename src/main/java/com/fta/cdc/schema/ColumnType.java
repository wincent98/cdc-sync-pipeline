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

    /** Outcome of comparing an upstream column type with the type the pipeline is using. */
    public enum Evolution {
        /** Identical type, no special handling required. */
        NONE,
        /** Silent widening (INT -> BIGINT or a longer VARCHAR), old data keeps fitting. */
        WIDENED,
        /** Narrowing or an unrelated type change; the event must be refused rather than truncated. */
        INCOMPATIBLE
    }

    /**
     * Classifies an upstream DDL change of a column whose previous type was {@code previous}
     * into this type.
     */
    public Evolution evolutionFrom(ColumnType previous) {
        if (this.equals(previous)) {
            return Evolution.NONE;
        }
        if (previous.kind == Kind.INT && this.kind == Kind.BIGINT) {
            return Evolution.WIDENED;
        }
        if (previous.kind == Kind.VARCHAR && this.kind == Kind.VARCHAR && this.length >= previous.length) {
            return Evolution.WIDENED;
        }
        return Evolution.INCOMPATIBLE;
    }

    /**
     * Checks whether a decoded binlog value can be stored under this column type without
     * truncation or overflow. Widened upstream values (e.g. a large BIGINT arriving while
     * the column is still INT, or a string longer than the VARCHAR length) do not fit and
     * the carrying event is rejected as an incompatible narrowing.
     */
    public boolean accepts(Object value) {
        if (value == null) {
            return true;
        }
        switch (kind) {
            case INT:
                if (value instanceof Integer) {
                    return true;
                }
                if (value instanceof Long) {
                    return ((Long) value) <= Integer.MAX_VALUE && ((Long) value) >= Integer.MIN_VALUE;
                }
                return false;
            case BIGINT:
                return value instanceof Long || value instanceof Integer;
            case VARCHAR:
                return value instanceof String && ((String) value).length() <= length;
            default:
                return false;
        }
    }
}
