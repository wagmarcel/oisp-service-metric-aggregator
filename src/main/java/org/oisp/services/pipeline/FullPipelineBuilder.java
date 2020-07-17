package org.oisp.services.pipeline;


import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;
import org.apache.beam.sdk.Pipeline;
import org.oisp.services.conf.Config;
import org.oisp.services.transformation.KafkaSourceObservationsProcessor;
import org.oisp.services.utils.LogHelper;
import org.slf4j.Logger;
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

    static class StringToKVFn extends DoFn<Long, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> outputKv = KV.<String, String>of("", "rules-engine");
            c.output(outputKv);
        }
    }

}
