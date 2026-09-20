package com.fta.cdc.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Holds events the pipeline refuses to apply. */
public class DeadLetterQueue {

    private final List<DeadLetter> letters = Collections.synchronizedList(new ArrayList<>());

    public void put(ChangeEvent event, String reason) {
        letters.add(new DeadLetter(event, reason));
    }

    public List<DeadLetter> letters() {
        synchronized (letters) {
            return new ArrayList<>(letters);
        }
    }

    public int size() {
        return letters.size();
    }
}
