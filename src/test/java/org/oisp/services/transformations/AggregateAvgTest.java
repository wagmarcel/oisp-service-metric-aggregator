package org.oisp.services.transformations;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;
import org.oisp.services.collections.AggregatedObservation;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MapAggregatedObservationToValue extends DoFn<AggregatedObservation, Double> implements Serializable {
    @ProcessElement public void processElement(ProcessContext c) {
        Observation obs = c.element().getObservation();
        c.output(Double.parseDouble(obs.getValue()));
    }
    public MapAggregatedObservationToValue(){}
}

class AggregateAvgTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    void aggregateAvgTest() {

        Aggregator aggregator = new Aggregator(Aggregator.AggregatorType.AVG, Aggregator.AggregatorUnit.minutes);
        Observation observation1 = new Observation();
        observation1.setValue("1.0");
        observation1.setDataType("Number");
        Observation observation2 = new Observation();
        observation2.setValue("2.0");
        observation2.setDataType("Number");
        Observation observation3 = new Observation();
        observation3.setValue("3.0");
        observation3.setDataType("Number");
        AggregatedObservation aggObs = new AggregatedObservation(observation2, aggregator);
        Iterable<Observation> itObs = Arrays.asList(observation1, observation2, observation3);
        KV<String, Iterable<Observation>> input = KV.of("key", itObs);
        PCollection<AggregatedObservation> out = pipeline.apply("Create Input", Create.of(input))
                .apply("test AggregateAvg", ParDo.of(new AggregateAvg(aggregator)));
        PCollection<Double> out2 = out
                .apply("extract aggregation value", ParDo.of(
                        new MapAggregatedObservationToValue())
                );

        PAssert.that(out2).containsInAnyOrder(2.0);
        pipeline.run().waitUntilFinish();
    }
}