package org.oisp.services.windows;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PartitioningWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;

import org.joda.time.Instant;
import org.joda.time.Duration;

public final class FullTimeInterval extends PartitioningWindowFn<KV<String, Observation>, IntervalWindow> {
    private final Aggregator aggregator;

    private FullTimeInterval(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public static FullTimeInterval withAggregator(Aggregator aggregator) {
        return new FullTimeInterval(aggregator);
    }

    @Override
    public IntervalWindow assignWindow(Instant timestamp) {

        Instant startTimeOfWindow;
        startTimeOfWindow = aggregator.getWindowStartTime(timestamp);
        Duration windowDuration;
        windowDuration = aggregator.getWindowDuration();
        IntervalWindow window = new IntervalWindow(startTimeOfWindow, windowDuration);
        return window;
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return false;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }
}
