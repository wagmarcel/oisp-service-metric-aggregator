package org.oisp.services.utils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.common.serialization.Deserializer;
import org.oisp.services.collections.Observation;
import org.oisp.services.collections.ObservationList;

import java.util.ArrayList;
import java.util.List;

public class ObservationDeserializer implements Deserializer<ObservationList> {

    public ObservationDeserializer(){}
    public void configure(java.util.Map<java.lang.String,?> configs,
                          boolean isKey) {
    }

    public void close(){
    }

    public ObservationList deserialize(java.lang.String topic,
                                       byte[] data) {
        Gson g = new Gson();
        List<Observation> observations = new ArrayList<Observation>();
            try {
                Observation observation = g.fromJson(new String(data), new TypeToken<Observation>() {
                }.getType());
                observations.add(observation);
            } catch (JsonSyntaxException e) {
                //LOG.debug("Parsing single observation failed. Now trying to parse List<Observation>: " + e);
                observations = g.fromJson(new String(data), new TypeToken<List<Observation>>() {
                }.getType());
            }
        ObservationList obsList = new ObservationList();
        obsList.setObservationList(observations);
        return obsList;
    }
}