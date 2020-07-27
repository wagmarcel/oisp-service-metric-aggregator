package org.oisp.services.transformations;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.services.collections.AggregatedObservation;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;

import java.io.Serializable;
import java.util.Iterator;

public class AggregateAvg extends DoFn<KV<String, Iterable<Observation>>, AggregatedObservation> {

    Aggregator aggregator;

    public AggregateAvg(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Iterable<Observation> itObs  = c.element().getValue();
        Observation firstObs = itObs.iterator().next();
        if (firstObs.isNumber()) {
            Long sum = 0l;
            Double accum = 0.0;
            for (Observation obs : itObs) {
                accum += Double.parseDouble(obs.getValue());
                sum++;
            }
            Observation newObs = new Observation(itObs.iterator().next());
            Double avg = accum / sum;
            newObs.setValue(avg.toString());
            c.output(new AggregatedObservation(newObs, aggregator));
        }
    }
}
