package org.oisp.services.dataStructures;

public class Aggregator {
    public enum AggregatorType{
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
    AggregatorType type;
    AggregatorUnit unit;
    Long value;
}
