package org.oisp.services.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.services.collections.AggregatedObservation;
import org.oisp.services.collections.Observation;
import org.oisp.services.conf.Config;
import org.oisp.services.dataStructures.Aggregator;
import java.util.Map;
import org.joda.time.Instant;

public class SendObservation extends DoFn<AggregatedObservation, KV<String, Observation>> {

    String serviceName;
    public SendObservation(Map<String, Object> config) {
        this.serviceName = (String)config.get(Config.SERVICE_NAME);
    }
    @ProcessElement
    public void processElement(ProcessContext c) {
        AggregatedObservation aggrObservation = c.element();
        Observation obs = new Observation(aggrObservation.getObservation());
        Aggregator aggr = aggrObservation.getAggregator();
        String cid = aggrObservation.getObservation().getCid();
        obs.setCid(cid + "." + serviceName + "." + aggr.getType() + "." + aggr.getUnit());
        Long fullTimeMillis = aggr.getWindowStartTime(Instant
                                .ofEpochMilli(aggrObservation.getObservation().getOn())).getMillis();
        Long durationMillis = Math.round(aggr.getWindowDuration().getMillis() / 2.0);
        Long timestampMillis = fullTimeMillis + durationMillis;
        obs.setOn(timestampMillis);
        obs.setSystemOn(timestampMillis);
        c.output(KV.of(obs.getCid(), obs));
    }
}
