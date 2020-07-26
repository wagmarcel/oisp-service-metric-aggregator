package org.oisp.services.utils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.oisp.services.collections.Observation;
import org.oisp.services.collections.ObservationList;

import java.util.ArrayList;
import java.util.List;

public class ObservationSerializer implements Serializer<ObservationList> {

    public ObservationSerializer(){}
    public void configure(java.util.Map<String,?> configs,
                          boolean isKey) {
    }

    public void close(){
    }

    public byte[] serialize(String topic, ObservationList observationList) {
        Gson g = new Gson();
        String output;
        output = g.toJson(observationList.getObservationList());
        return output.getBytes();
    }
}