package com.fta.cdc.offset;

import java.util.concurrent.atomic.AtomicLong;

/** Durable consumer position. On restart the pipeline resumes from committed() + 1. */
public class OffsetStore {

    private final AtomicLong committed = new AtomicLong(-1L);

    public void commit(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        committed.set(offset);
    }

    public long committed() {
        return committed.get();
    }

    public void reset() {
        committed.set(-1L);
    }
}
