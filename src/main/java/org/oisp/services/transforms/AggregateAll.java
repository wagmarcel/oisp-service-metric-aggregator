package org.oisp.services.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.oisp.services.collections.AggregatedObservation;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;
import org.oisp.services.utils.LogHelper;
import org.slf4j.Logger;

public class AggregateAll extends DoFn<KV<String, Iterable<Observation>>, AggregatedObservation> {

    Aggregator aggregator;

    public AggregateAll(Aggregator aggregator) {
        this.aggregator = aggregator;
    }
    Logger LOG = LogHelper.getLogger(AggregateAll.class);
    @ProcessElement
    public void processElement(ProcessContext c, PaneInfo paneInfo) {
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

            Double avg = accum / count;
            if (aggregator.getType() == Aggregator.AggregatorType.AVG || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                Observation avgObs = new Observation(itObs.iterator().next());
                avgObs.setValue(avg.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.AVG, aggregator.getUnit());
                c.output(new AggregatedObservation(avgObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.SUM || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                Observation sumObs = new Observation(itObs.iterator().next());
                sumObs.setValue(accum.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.SUM, aggregator.getUnit());
                LOG.debug("Aggregated SUM value: {}", sumObs.getValue());
                c.output(new AggregatedObservation(sumObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.MIN || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                Observation minObs = new Observation(itObs.iterator().next());
                minObs.setValue(min.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.MIN, aggregator.getUnit());
                LOG.debug("Aggregated MIN value: {}", minObs.getValue());
                c.output(new AggregatedObservation(minObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.MAX || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                Observation maxObs = new Observation(itObs.iterator().next());
                maxObs.setValue(max.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.MAX, aggregator.getUnit());
                LOG.debug("Aggregated MAX value: {}", maxObs.getValue());
                c.output(new AggregatedObservation(maxObs, newAggr));
            }
            if (aggregator.getType() == Aggregator.AggregatorType.COUNT || aggregator.getType() == Aggregator.AggregatorType.ALL) {
                Observation countObs = new Observation(itObs.iterator().next());
                countObs.setValue(count.toString());
                Aggregator newAggr = new Aggregator(Aggregator.AggregatorType.COUNT, aggregator.getUnit());
                LOG.debug("Aggregated COUNT value: {}", countObs.getValue());
                c.output(new AggregatedObservation(countObs, newAggr));
            }
        }
    }
}
