package org.oisp.services.transforms;


import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.oisp.services.collections.Observation;
import org.oisp.services.collections.ObservationList;
import org.oisp.services.conf.Config;

import java.util.Map;

// Distribute elements with cid key
// Filter out already aggregated values
public class KafkaToFilteredObservationFn extends DoFn<KafkaRecord<String, ObservationList>, KV<String, Observation>> {
    private String serviceName;
    public KafkaToFilteredObservationFn(Map<String, Object> conf) {
        serviceName = (String) conf.get(Config.SERVICE_NAME);
    }
    @ProcessElement
    public void processElement(ProcessContext c, @Timestamp Instant inputTimestamp) {
        ObservationList observations = c.element().getKV().getValue();

        observations.getObservationList().forEach((obs) -> {
            if (!obs.getCid().contains(serviceName)) {
                Instant timestamp = new Instant().withMillis(obs.getOn());
                Instant now = Instant.now();
                c.output(KV.of(obs.getCid(), obs));
            }
        });
    }
}
