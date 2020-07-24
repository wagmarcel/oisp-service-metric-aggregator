package org.oisp.services.pipeline;


import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;
import org.apache.beam.sdk.Pipeline;
import org.joda.time.LocalDateTime;
import org.oisp.services.collection.Observation;
import org.oisp.services.conf.Config;
import org.oisp.services.dataStructures.Aggregator;
import org.oisp.services.transformation.KafkaSourceObservationsProcessor;
import org.oisp.services.utils.LogHelper;
import org.oisp.services.windows.FullTimeInterval;
import org.slf4j.Logger;

import org.joda.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public final class FullPipelineBuilder {

    private static final Logger LOG = LogHelper.getLogger(FullPipelineBuilder.class);
    private FullPipelineBuilder() {
    }

    public static Pipeline build(PipelineOptions options, Map<String, Object> conf) {
        Pipeline p = Pipeline.create(options);



        //Observation Pipeline
        //Map observations to rules
        //Process rules for Basic, Timebased and Statistics
        KafkaSourceObservationsProcessor observationsKafka = new KafkaSourceObservationsProcessor(conf);
        PCollection<KV<String, Observation>> obs = p.apply(observationsKafka.getTransform())
                .apply(ParDo.of(new KafkaToObservationFn()))
                        .apply(Window.configure().<KV<String, Observation>>into(
                        FullTimeInterval.withUnit(Aggregator.AggregatorUnit.minutes))
                                //FixedWindows.of(Duration.standardSeconds(10)))
                );
        PCollection<KV<String, Iterable<Observation>>> gbk = obs.apply(GroupByKey.<String, Observation>create());
        PCollection<Long> sizes = gbk.apply(ParDo.of(new PrintGBKFn()));


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


    // Print out gbk results
    static class PrintGBKFn extends DoFn<KV<String, Iterable<Observation>>, Long> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String key = c.element().getKey();
            Iterable<Observation> observations = c.element().getValue();
            Iterator<Observation> it = observations.iterator();
            Integer elements = Iterators.size(it);
            System.out.println("key " + key + " size " + elements);
            c.output(Long.valueOf(elements));
        }
    }

    // Distribute elements with cid key
    static class KafkaToObservationFn extends DoFn<KafkaRecord<String, byte[]>, KV<String, Observation>> {
        @ProcessElement
        public void processElement(ProcessContext c, @Timestamp Instant inputTimestamp) {
            KafkaRecord<String, byte[]> record = c.element();
            Gson g = new Gson();
            List<Observation> observations = new ArrayList<Observation>();
            try {
                Observation observation = g.fromJson(new String(record.getKV().getValue()), new TypeToken<Observation>() {
                }.getType());
                observations.add(observation);
            } catch (JsonSyntaxException e) {
                LOG.debug("Parsing single observation failed. Now trying to parse List<Observation>: " + e);
                observations = g.fromJson(new String(record.getKV().getValue()), new TypeToken<List<Observation>>() {
                }.getType());
            }

            observations.forEach((obs) -> {
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
