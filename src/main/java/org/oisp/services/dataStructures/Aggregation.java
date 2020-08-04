package org.oisp.services.dataStructures;

import java.util.List;

public class Aggregation {

    private Aggregator aggregator;
    private List<AggregatedValues> aggregatedValuesList;

    public Aggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public List<AggregatedValues> getAggregatedValuesList() {
        return aggregatedValuesList;
    }

    public void setAggregatedValuesList(List<AggregatedValues> aggregatedValuesList) {
        this.aggregatedValuesList = aggregatedValuesList;
    }
}
