package org.oisp.services.utils;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;
import org.oisp.services.collections.Observation;

public class ObservationSerializer implements Serializer<Observation> {

    public ObservationSerializer() {
    }
    public void configure(java.util.Map<String, ?> configs,
                          boolean isKey) {
    }

    public void close() {
    }

    public byte[] serialize(String topic, Observation observation) {
        Gson g = new Gson();
        String output;
        output = g.toJson(observation);
        return output.getBytes();
    }
}