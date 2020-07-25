package org.oisp.services.windows;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.oisp.services.collections.Observation;
import org.oisp.services.dataStructures.Aggregator;

//import java.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;

public class FullTimeInterval extends WindowFn<KV<String, Observation>, IntervalWindow> {
    private final Aggregator aggregator;

    private FullTimeInterval(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public static FullTimeInterval withAggregator(Aggregator aggregator) {
        return new FullTimeInterval(aggregator);
    }

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) {

        KV<String, Observation> obs = c.element();
        Instant timeStampOfCurrentSample = c.timestamp();
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(timeStampOfCurrentSample.toDate());
        gc.set(Calendar.SECOND, 0);
        gc.set(Calendar.MILLISECOND, 0);
        Duration windowDuration;
        Instant startTimeOfWindow;
        switch(aggregator.getUnit()){
            case hours:
                windowDuration = Duration.standardHours (1);
                gc.set(Calendar.MINUTE, 0);
                break;
            case minutes:
            default:
                windowDuration = Duration.standardMinutes(1);
                break;
        }
        startTimeOfWindow = new Instant(gc.getTimeInMillis());
        return Arrays.asList(new IntervalWindow(startTimeOfWindow, windowDuration));
    }

    @Override
    public void mergeWindows(MergeContext c) {
        //System.out.printf("Marcel234 mergeWindows");

        // Overlap should not happen
    }

    @Override
    public boolean isCompatible(WindowFn<?,?> other) {
        return false;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        return null;
    }
}
