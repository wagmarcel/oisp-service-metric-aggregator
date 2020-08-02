package org.oisp.services.pipelines;


import com.google.common.collect.Iterators;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;
import org.apache.beam.sdk.Pipeline;
import org.oisp.services.collections.AggregatedObservation;
import org.oisp.services.collections.Observation;
import org.oisp.services.collections.ObservationList;
import org.oisp.services.conf.Config;
import org.oisp.services.dataStructures.Aggregator;
import org.oisp.services.transforms.*;
import org.oisp.services.utils.LogHelper;
import org.oisp.services.windows.FullTimeInterval;
import org.slf4j.Logger;

import org.joda.time.Instant;

import java.util.Iterator;
import java.util.Map;


import static org.apache.beam.sdk.Pipeline.create;


public final class FullPipelineBuilder {

    private static final Logger LOG = LogHelper.getLogger(FullPipelineBuilder.class);
    private FullPipelineBuilder() {
    }

    public static Pipeline build(PipelineOptions options, Map<String, Object> conf) {
        Pipeline p = create(options);



        //Observation Pipeline
        //Map observations to aggregated values
        // ----------------    ----------------------    ---------------------     ------------------------
        // | Kafka Source | => | Filter Observation | => | AggregationWindow | =>  | Group windows by keys | =>
        //  ---------------    ----------------------    ---------------------     ------------------------
        //
        // -------------    -----------------------------------    --------------
        // | Aggegator | => | Prepare Observation for sending | => | Kafka Sink |
        // -------------    -----------------------------------    --------------
        //Process rules for Basic, Timebased and Statistics
        KafkaObservationsSourceProcessor observationsKafka = new KafkaObservationsSourceProcessor(conf);
        KafkaObservationSink kafkaSink = new KafkaObservationsSinkProcessor(conf);

        PCollection<KV<String, Observation>> observations = p.apply("Kafka Source", observationsKafka.getTransform())
                .apply("Filter Observation", ParDo.of(new KafkaToFilteredObservationFn(conf)));

        // window for minutes
        PCollection<KV<String, Observation>> observationsPerMinute = observations
                .apply("Aggregation Window for minutes", Window.configure().<KV<String, Observation>>into(
                FullTimeInterval.withAggregator(
                        new Aggregator(Aggregator.AggregatorType.NONE, Aggregator.AggregatorUnit.minutes))
        ));
        PCollection<KV<String, Iterable<Observation>>> groupedObservationsPerMinute = observationsPerMinute
                .apply("Group windows by keys for minutes", GroupByKey.<String, Observation>create());

        // window for hours
        PCollection<KV<String, Observation>> observationsPerHour = observations
                .apply("Aggregation Window for hours", Window.configure().<KV<String, Observation>>into(
                FullTimeInterval.withAggregator(
                        new Aggregator(Aggregator.AggregatorType.NONE, Aggregator.AggregatorUnit.hours))
        )/*.triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.standardHours(0)).accumulatingFiredPanes()*/
                );
        PCollection<KV<String, Iterable<Observation>>> groupedObservationsPerHour = observationsPerHour
                .apply("Group windows by keys for hours", GroupByKey.<String, Observation>create());

        // Apply aggregators
        // There are two windows, minutes and hours
        PCollection<AggregatedObservation> aggrPerHour = groupedObservationsPerMinute
                .apply("Aggregator", ParDo.of(
                        new AggregateAll(
                                new Aggregator(Aggregator.AggregatorType.ALL, Aggregator.AggregatorUnit.minutes))));
        PCollection<AggregatedObservation> aggrPerMinute = groupedObservationsPerHour
                .apply("Aggregator", ParDo.of(
                        new AggregateAll(
                                new Aggregator(Aggregator.AggregatorType.ALL, Aggregator.AggregatorUnit.hours))));
        // debugging output
        //aggrPerMinute.apply("Debug output", ParDo.of(new PrintAggregationResultFn()));
        aggrPerHour.apply("Debug output", ParDo.of(new PrintAggregationResultFn()));
        aggrPerMinute.apply("Debug output", ParDo.of(new PrintAggregationResultFn()));
        //groupedObservationsPerMinute.apply("Debug output", ParDo.of(new PrintGBKFn()));
       // groupedObservationsPerHour.apply("Debug output", ParDo.of(new PrintGBKFn()));

        // Prepare observations and send down the Kafka Sink
        aggrPerMinute.apply("Prepare Observation for sending", ParDo.of(new SendObservation(conf))).apply("Kafka Sink", kafkaSink.getTransform());
        aggrPerHour.apply("Prepare Observation for sending", ParDo.of(new SendObservation(conf))).apply("Kafka Sink", kafkaSink.getTransform());


        //Heartbeat Pipeline
        //Send regular Heartbeat to Kafka topic
        /*String serverUri = conf.get(Config.KAFKA_BOOTSTRAP_SERVERS).toString();
        System.out.println("serverUri:" + serverUri);
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(ParDo.of(new StringToKVFn()))
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers(serverUri)
                        .withTopic("heartbeat")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));*/
        return p;
    }

    static class PrintAggregationResultFn extends DoFn<AggregatedObservation, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element() != null) {
                Aggregator aggr = c.element().getAggregator();
                Observation obs = c.element().getObservation();
                System.out.println("Result of aggregator: aggr " + aggr.getType() + ", value: " + obs.getValue() +
                        ", key " + obs.getCid() + ", window(" + aggr.getWindowDuration() + ","
                        + aggr.getWindowStartTime(Instant.ofEpochMilli(obs.getOn())) + ") now:" + Instant.now());
                c.output(Long.valueOf(0));

            }
        }

    }

    // Print out gbk results
    static class PrintGBKFn extends DoFn<KV<String, Iterable<Observation>>, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            Iterable<Observation> observations = c.element().getValue();
            Iterator<Observation> it = observations.iterator();
            Integer elements = Iterators.size(it);
            System.out.print("key " + key + " size " + elements + "=> ");
            for(Iterator<Observation> iter = observations.iterator(); iter.hasNext(); ) {
                Observation obs = iter.next();
                if (!obs.isByteArray()) {
                    System.out.print(obs.getValue() + ", " + Instant.ofEpochMilli(obs.getOn()) + ";");
                } else {
                    System.out.print("*removed*");
                }
            }
            System.out.println("<= end");
            c.output(Long.valueOf(elements));
        }
    }

    // Distribute elements with cid key
    // Filter out already aggregated values
    /*static class KafkaToFilteredObservationFn extends DoFn<KafkaRecord<String, ObservationList>, KV<String, Observation>> {
        String serviceName;
        KafkaToFilteredObservationFn(Map<String, Object> conf) {
            serviceName = (String) conf.get(Config.SERVICE_NAME);
        }
        @ProcessElement
        public void processElement(ProcessContext c, @Timestamp Instant inputTimestamp) {
            ObservationList observations = c.element().getKV().getValue();

            observations.getObservationList().forEach((obs) -> {
                if (! obs.getCid().contains(serviceName)) {
                    Instant timestamp = new Instant().withMillis(obs.getOn());
                    Instant now = Instant.now();
                    c.output(KV.of(obs.getCid(), obs));
                }
            });
        }
    }*/

    static class StringToKVFn extends DoFn<Long, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> outputKv = KV.<String, String>of("", "rules-engine");
            c.output(outputKv);
        }
    }

}
