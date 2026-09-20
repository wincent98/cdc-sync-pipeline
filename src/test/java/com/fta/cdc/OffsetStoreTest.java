package com.fta.cdc;

import com.fta.cdc.offset.OffsetStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OffsetStoreTest {

    private OffsetStore store;

    @BeforeEach
    void setUp() {
        store = new OffsetStore();
    }

    @Test
    void startsBeforeTheFirstOffset() {
        assertEquals(-1L, store.committed());
    }

    @Test
    void remembersTheCommittedOffset() {
        store.commit(5L);
        assertEquals(5L, store.committed());
    }

    @Test
    void acceptsOffsetZero() {
        store.commit(0L);
        assertEquals(0L, store.committed());
    }

    @Test
    void rejectsANegativeOffset() {
        assertThrows(IllegalArgumentException.class, () -> store.commit(-2L));
    }

    @Test
    void lastCommitWins() {
        store.commit(3L);
        store.commit(8L);
        assertEquals(8L, store.committed());
    }

    @Test
    void resetGoesBackToTheStart() {
        store.commit(4L);
        store.reset();
        assertEquals(-1L, store.committed());
    }
}
