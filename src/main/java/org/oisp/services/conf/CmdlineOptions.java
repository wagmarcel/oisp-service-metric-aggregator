package org.oisp.services.conf;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface CmdlineOptions extends PipelineOptions {
    @Description("Kafka topic for metrics")
    @Default.String("")
    String getMetricsTopic();
    void setMetricsTopic(String value);

    @Description("Kafka Bootstrap Servers")
    @Default.String("")
    String getBootstrapServers();
    void setBootstrapServers(String value);

    @Description("Service Name")
    @Default.String("aggregator")
    String getServiceName();
    void setServiceName(String value);
}
