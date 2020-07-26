package org.oisp.services.pipelines;


import com.google.common.collect.Iterators;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
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
import org.oisp.services.transformations.AggregateAvg;
import org.oisp.services.transformations.KafkaSourceObservationsProcessor;
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
        //Map observations to rules
        //Process rules for Basic, Timebased and Statistics
        KafkaSourceObservationsProcessor observationsKafka = new KafkaSourceObservationsProcessor(conf);

        PCollection<KV<String, Observation>> obs = p.apply(observationsKafka.getTransform())
                .apply(ParDo.of(new KafkaToObservationFn()))
                .apply(Window.configure().<KV<String, Observation>>into(
                        FullTimeInterval.withAggregator(
                                new Aggregator(Aggregator.AggregatorType.NONE, Aggregator.AggregatorUnit.minutes))
                ));

        PCollection<KV<String, Iterable<Observation>>> gbk = obs.apply(GroupByKey.<String, Observation>create());
        PCollection<Long> sizes = gbk.apply(ParDo.of(new PrintGBKFn()));

        /*PCollection<KV<String, Iterable<Observation>>>  group = obs
                .apply(GroupByKey.<String, Observation>create());
        PCollection<Long> sizes = group.apply(ParDo.of(new PrintGBKFn()));*/

        PCollection<AggregatedObservation> aggr = gbk
                .apply(ParDo.of(
                        new AggregateAvg(
                                new Aggregator(Aggregator.AggregatorType.AVG, Aggregator.AggregatorUnit.minutes))));
        PCollection<Long> aggrValues = aggr.apply(ParDo.of(new PrintAggregationResultFn()));
        //aggr.setCoder(SerializableCoder.of(KV<String.class, Observation.class>));

        //Heartbeat Pipeline
        //Send regular Heartbeat to Kafka topic
        String serverUri = conf.get(Config.KAFKA_BOOTSTRAP_SERVERS).toString();
        System.out.println("serverUri:" + serverUri);
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(ParDo.of(new StringToKVFn()))
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers(serverUri)
                        .withTopic("heartbeat")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));
        return p;
    }

    static class PrintAggregationResultFn extends DoFn<AggregatedObservation, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element() != null) {
                Aggregator aggr = c.element().getAggregator();
                Observation obs = c.element().getObservation();
                System.out.println("Result of aggregator: aggr " + aggr.getType() + ", value: " + obs.getValue() +
                        ", key " + obs.getCid() + ", window(" +
                        aggr.getWindowDuration());
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
    static class KafkaToObservationFn extends DoFn<KafkaRecord<String, ObservationList>, KV<String, Observation>> {
        @ProcessElement
        public void processElement(ProcessContext c, @Timestamp Instant inputTimestamp) {
            /*KafkaRecord<String, byte[]> record = c.element();
            Gson g = new Gson();*/
            ObservationList observations = c.element().getKV().getValue();
            /*try {
                Observation observation = g.fromJson(new String(record.getKV().getValue()), new TypeToken<Observation>() {
                }.getType());
                observations.add(observation);
            } catch (JsonSyntaxException e) {
                LOG.debug("Parsing single observation failed. Now trying to parse List<Observation>: " + e);
                observations = g.fromJson(new String(record.getKV().getValue()), new TypeToken<List<Observation>>() {
                }.getType());
            }*/

            observations.getObservationList().forEach((obs) -> {
                Instant timestamp = new Instant().withMillis(obs.getOn());
                Instant now = Instant.now();
                c.output(KV.of(obs.getCid(), obs));
            });
        }
    }

    static class StringToKVFn extends DoFn<Long, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> outputKv = KV.<String, String>of("", "rules-engine");
            c.output(outputKv);
        }
    }

}
