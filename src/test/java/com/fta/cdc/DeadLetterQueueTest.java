package com.fta.cdc;

import com.fta.cdc.event.ChangeEvent;
import com.fta.cdc.event.DeadLetterQueue;
import com.fta.cdc.event.Op;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadLetterQueueTest {

    private DeadLetterQueue queue;

    @BeforeEach
    void setUp() {
        queue = new DeadLetterQueue();
    }

    private static ChangeEvent ev(long offset, String pk) {
        return new ChangeEvent(offset, "orders", pk, Op.INSERT, 1L, new LinkedHashMap<>());
    }

    @Test
    void startsEmpty() {
        assertEquals(0, queue.size());
        assertTrue(queue.letters().isEmpty());
    }

    @Test
    void keepsTheEventAndTheReason() {
        queue.put(ev(1L, "O-1"), "narrowing currency");
        assertEquals("O-1", queue.letters().get(0).event().primaryKey());
        assertEquals("narrowing currency", queue.letters().get(0).reason());
    }

    @Test
    void keepsInsertionOrder() {
        queue.put(ev(1L, "O-1"), "r1");
        queue.put(ev(2L, "O-2"), "r2");
        assertEquals("O-1", queue.letters().get(0).event().primaryKey());
        assertEquals("O-2", queue.letters().get(1).event().primaryKey());
    }

    @Test
    void countsEveryLetter() {
        queue.put(ev(1L, "O-1"), "r");
        queue.put(ev(2L, "O-1"), "r");
        assertEquals(2, queue.size());
    }

    @Test
    void returnsADefensiveCopy() {
        queue.put(ev(1L, "O-1"), "r");
        queue.letters().clear();
        assertEquals(1, queue.size());
    }

    @Test
    void reasonShowsUpInToString() {
        queue.put(ev(1L, "O-1"), "narrowing currency");
        assertTrue(queue.letters().get(0).toString().contains("narrowing currency"));
    }
}
