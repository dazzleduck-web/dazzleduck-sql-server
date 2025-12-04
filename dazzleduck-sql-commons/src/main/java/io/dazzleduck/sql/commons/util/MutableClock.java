package io.dazzleduck.sql.commons.util;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class MutableClock extends Clock {

    private Instant currentInstant;
    private final ZoneId zoneId;

    public MutableClock(Instant initialInstant, ZoneId zoneId) {
        this.currentInstant = initialInstant;
        this.zoneId = zoneId;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public synchronized Clock withZone(ZoneId zone) {
        return new MutableClock(currentInstant, zone);
    }

    @Override
    public synchronized Instant instant() {
        return currentInstant;
    }

    public synchronized void setInstant(Instant newInstant) {
        this.currentInstant = newInstant;
    }

    public synchronized void advanceBy(Duration duration) {
        this.currentInstant = this.currentInstant.plus(duration);
    }
}
