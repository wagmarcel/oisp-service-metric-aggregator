package org.oisp.services.dataStructures;

import java.io.Serializable;

public class Aggregator implements Serializable {
    public enum AggregatorType{
        NONE,
        AVG,
        SUM,
        COUNT,
        MAX,
        MIN
    }
    public enum AggregatorUnit{
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
}
