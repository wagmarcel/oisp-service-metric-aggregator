package org.oisp.services.dataStructures;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class Aggregator implements Serializable {
    public enum AggregatorType {
        NONE,
        ALL,
        AVG,
        SUM,
        COUNT,
        MAX,
        MIN
    }
    public enum AggregatorUnit {
        minutes,
        hours
    }
    private AggregatorType type;
    private AggregatorUnit unit;


    public Aggregator(AggregatorType type, AggregatorUnit unit) {
        this.type = type;
        this.unit = unit;
    }

    public AggregatorType getType() {
        return type;
    }

    public void setType(AggregatorType type) {
        this.type = type;
    }

    public AggregatorUnit getUnit() {
        return unit;
    }

    public void setUnit(AggregatorUnit unit) {
        this.unit = unit;
    }

    public Instant getWindowStartTime(Instant time) {
        Instant startTimeOfWindow;
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(time.toDate());
        gc.set(Calendar.SECOND, 0);
        gc.set(Calendar.MILLISECOND, 0);
        switch (unit) {
            case hours:
                gc.set(Calendar.MINUTE, 0);
                break;
            case minutes:
            default:
                break;
        }
        startTimeOfWindow = new Instant(gc.getTimeInMillis());
        return startTimeOfWindow;
    }

    public Duration getWindowDuration() {
        Duration windowDuration;
        switch (unit) {
            case hours:
                windowDuration = Duration.standardHours(1);
                break;
            case minutes:
            default:
                windowDuration = Duration.standardMinutes(1);
                break;
        }
        return windowDuration;
    }
}
