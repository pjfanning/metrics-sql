package com.github.gquintana.metrics.sql;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.core.lang.Nullable;

public class MeterRegistryHelper {
    private final static MockClock clock = new MockClock();

    private final static DropwizardConfig config = new DropwizardConfig() {
        @Override
        public String prefix() {
            return "dropwizard";
        }

        @Override
        @Nullable
        public String get(String key) {
            return null;
        }
    };


    static DropwizardMeterRegistry createDropwizardMeterRegistry() {
        return new DropwizardMeterRegistry(config, new MetricRegistry(), HierarchicalNameMapper.DEFAULT, clock) {
            @Override
            protected Double nullGaugeValue() {
                return Double.NaN;
            }
        };
    }
}
