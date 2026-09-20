package com.fta.cdc.event;

public final class DeadLetter {

    private final ChangeEvent event;
    private final String reason;

    public DeadLetter(ChangeEvent event, String reason) {
        this.event = event;
        this.reason = reason;
    }

    public ChangeEvent event() {
        return event;
    }

    public String reason() {
        return reason;
    }

    @Override
    public String toString() {
        return "DeadLetter{" + event.primaryKey() + "@" + event.offset() + ", " + reason + '}';
    }
}
