package org.oisp.services.collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ObservationList implements Serializable {
    // needed to workaround the fact that get class of List<Observation> fails
    public List<Observation> getObservationList() {
        return observationList;
    }

    public ObservationList() {
        observationList = new ArrayList<Observation>();
    }
    public void setObservationList(List<Observation> observationList) {
        this.observationList = observationList;
    }

    private List<Observation> observationList;
}
