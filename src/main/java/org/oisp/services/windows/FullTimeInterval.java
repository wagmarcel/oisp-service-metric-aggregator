package org.oisp.services.windows;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;

//import java.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.Collection;

public class FullTimeInterval extends PartitioningWindowFn<KV<String, Observation>, IntervalWindow> {
    private final Aggregator aggregator;

    private FullTimeInterval(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public static FullTimeInterval withAggregator(Aggregator aggregator) {
        return new FullTimeInterval(aggregator);
    }

    @Override
    public IntervalWindow assignWindow(Instant timestamp) {

        //KV<String, Observation> obs = c.element();
        //Instant timeStampOfCurrentSample = c.timestamp();
        Instant startTimeOfWindow;
        startTimeOfWindow = aggregator.getWindowStartTime(timestamp);
        Duration windowDuration;
        windowDuration = aggregator.getWindowDuration();
        IntervalWindow window = new IntervalWindow(startTimeOfWindow, windowDuration);
        return window;
    }

    /*@Override
    public Collection<IntervalWindow> assignWindows(WindowFn.AssignContext c)) {

        KV<String, Observation> obs = c.element();
        Instant timeStampOfCurrentSample = c.timestamp();
        Instant startTimeOfWindow;
        startTimeOfWindow = aggregator.getWindowStartTime(timeStampOfCurrentSample);
        Duration windowDuration;
        windowDuration = aggregator.getWindowDuration();
        return Arrays.asList(new IntervalWindow(startTimeOfWindow, windowDuration));
    }*/

    @Override
    public boolean isCompatible(WindowFn<?,?> other) {
        return false;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }
}
