package org.oisp.services.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.services.collections.AggregatedObservation;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;

public class AggregateAll extends DoFn<KV<String, Iterable<Observation>>, AggregatedObservation> {

    Aggregator aggregator;

    public AggregateAll(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Iterable<Observation> itObs  = c.element().getValue();
        Observation firstObs = itObs.iterator().next();
        if (firstObs.isNumber()) {
            Long count = 0l;
            Double min = Double.MAX_VALUE;
            Double max = Double.MIN_VALUE;
            Double accum = 0.0;
            for (Observation obs : itObs) {
                Double value = Double.parseDouble(obs.getValue());
                accum += value;
                if (value < min) {
                    min = value;
                }
                if (value > max) {
                    max = value;
                }
                count++;
            }
            Observation newObs = new Observation(itObs.iterator().next());
            Double avg = accum / count;
            if (aggregator.getType() == Aggregator.AggregatorType.AVG || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                newObs.setValue(avg.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.AVG, aggregator.getUnit());
                c.output(new AggregatedObservation(newObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.SUM || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                newObs.setValue(accum.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.SUM, aggregator.getUnit());
                c.output(new AggregatedObservation(newObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.MIN || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                newObs.setValue(min.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.MIN, aggregator.getUnit());
                c.output(new AggregatedObservation(newObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.MAX || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                newObs.setValue(max.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.MAX, aggregator.getUnit());
                c.output(new AggregatedObservation(newObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.COUNT || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                newObs.setValue(count.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.COUNT, aggregator.getUnit());
                c.output(new AggregatedObservation(newObs, newAggr));
            }
        }
    }
}
