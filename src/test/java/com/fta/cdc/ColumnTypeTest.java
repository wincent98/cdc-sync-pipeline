package com.fta.cdc;

import com.fta.cdc.schema.ColumnType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ColumnTypeTest {

    @Test
    void buildsIntType() {
        assertEquals(ColumnType.Kind.INT, ColumnType.intType().kind());
    }

    @Test
    void buildsBigintType() {
        assertEquals(ColumnType.Kind.BIGINT, ColumnType.bigintType().kind());
    }

    @Test
    void varcharKeepsItsLength() {
        ColumnType t = ColumnType.varchar(32);
        assertEquals(ColumnType.Kind.VARCHAR, t.kind());
        assertEquals(32, t.length());
    }

    @Test
    void rejectsANonPositiveVarcharLength() {
        assertThrows(IllegalArgumentException.class, () -> ColumnType.varchar(0));
    }

    @Test
    void equalityCoversKindAndLength() {
        assertEquals(ColumnType.varchar(8), ColumnType.varchar(8));
        assertNotEquals(ColumnType.varchar(8), ColumnType.varchar(16));
        assertNotEquals(ColumnType.intType(), ColumnType.bigintType());
        assertEquals(ColumnType.intType().hashCode(), ColumnType.intType().hashCode());
    }

    @Test
    void printsAReadableName() {
        assertEquals("VARCHAR(16)", ColumnType.varchar(16).toString());
        assertEquals("BIGINT", ColumnType.bigintType().toString());
    }
}
