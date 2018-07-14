package com.github.gquintana.metrics.sql;

import io.micrometer.core.instrument.Timer;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class TimeObservation implements Closeable {

    private final Timer timer;
    private final long startTime;

    public TimeObservation(Timer timer, long startTime) {
        this.timer = timer;
        this.startTime = startTime;
    }

    @Override
    public void close() {
        timer.record(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }
}
