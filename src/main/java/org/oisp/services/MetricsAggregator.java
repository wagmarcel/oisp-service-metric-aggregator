package org.oisp.services;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.oisp.services.conf.CmdlineOptions;
import org.oisp.services.conf.Config;
import org.oisp.services.pipelines.FullPipelineBuilder;

import java.util.HashMap;


/**
 * RuleEngineBuild - creates different pipelines for Rule-engine Example
 */



public abstract class MetricsAggregator {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CmdlineOptions.class);

        PipelineOptionsFactory.register(CmdlineOptions.class);
        Pipeline fullPipeline;

        //read json config from ENVIRONMENT - needed because stupid mvn cannot read JSON from cmdline. Unbelievable, but true.
        String metricsTopic = ((CmdlineOptions) options).getMetricsTopic();
        String bootstrapServers = ((CmdlineOptions) options).getBootstrapServers();

        HashMap<String, Object> config = new HashMap<>();

        config.put(Config.KAFKA_METRICS_TOPIC, metricsTopic);
        config.put(Config.KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);


        fullPipeline = FullPipelineBuilder.build(options, config);
        fullPipeline.run().waitUntilFinish();
    }
}
