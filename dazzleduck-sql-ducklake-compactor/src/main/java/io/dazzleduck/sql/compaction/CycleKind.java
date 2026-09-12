package io.dazzleduck.sql.compaction;

import java.util.Locale;

/**
 * The kinds of scheduled cycle this service runs.
 *
 * <p>Typing the vocabulary is what keeps the failure counters honest: a misspelled string would
 * silently create an accumulator that no meter is registered for and no total reads back.
 */
public enum CycleKind {
    MINOR,
    MAJOR,
    HOUSEKEEPING;

    /** The value carried by the {@code type} meter tag. */
    public String tag() {
        return name().toLowerCase(Locale.ROOT);
    }
}
