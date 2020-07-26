package org.oisp.services.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.hamcrest.collection.IsMapContaining;
import org.junit.jupiter.api.Test;
import org.oisp.services.collections.Observation;
import org.oisp.services.collections.ObservationList;

import java.util.*;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ObservationSerializerTest {

    @Test
    public void testSerialize(){
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("key1", "value1");
        attributes.put("key2", "value2");
        Observation observation = new Observation();
        observation.setValue("test");
        observation.setCid("4afd14aa-e56f-4d53-aa22-138121823bee");
        observation.setAid("a67e051f-8443-4553-b1b9-de22a15df904");
        observation.setDataTypeString();
        observation.setOn(1000000700000l);
        observation.setSystemOn(1000000800000l);
        observation.setAttributes(attributes);
        observation.setLoc(Arrays.asList(100.1, 200.2));
        List<Observation> listObservation = Arrays.asList(observation);
        ObservationList observationList = new ObservationList();
        observationList.setObservationList(listObservation);

        byte[] serialized = new ObservationSerializer().serialize("topic", observationList);

        Gson g = new Gson();
        List<Observation> observationList2 = new ArrayList<>();

        observationList2 = g.fromJson(new String(serialized), new TypeToken<List<Observation>>() {
        }.getType());

        Observation deserializedObservation = observationList2.get(0);
        assertEquals(observation.getValue(), deserializedObservation.getValue());
        assertEquals(observation.getCid(), deserializedObservation.getCid());
        assertEquals(observation.getAid(), deserializedObservation.getAid());
        assertEquals(observation.getDataType(), deserializedObservation.getDataType());
        assertEquals(observation.getOn(), deserializedObservation.getOn());
        assertEquals(observation.getSystemOn(), deserializedObservation.getSystemOn());
        assertThat(deserializedObservation.getAttributes(), IsMapContaining.hasEntry("key1", "value1"));
        assertThat(deserializedObservation.getAttributes(), IsMapContaining.hasEntry("key2", "value2"));
        assertEquals(deserializedObservation.getLoc().size(), 2);
        assertThat(deserializedObservation.getLoc(), hasItem(100.1));
        assertThat(deserializedObservation.getLoc(), hasItem(200.2));
    }
}