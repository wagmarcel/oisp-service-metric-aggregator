package org.oisp.services.collections;

import org.oisp.services.dataStructures.Aggregator;

import java.io.Serializable;

public class AggregatedObservation implements Serializable {
    private Observation observation;
    private Aggregator aggregator;

    public AggregatedObservation(Observation observation, Aggregator aggregator) {
        this.observation = observation;
        this.aggregator = aggregator;
    }

    public AggregatedObservation() {
    }
    public Observation getObservation() {
        return observation;
    }

    public void setObservation(Observation observavtion) {
        this.observation = observavtion;
    }

    public Aggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregator = aggregator;
    }
}
