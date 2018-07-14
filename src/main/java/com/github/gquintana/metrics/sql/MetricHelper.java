package com.github.gquintana.metrics.sql;

/*
 * #%L
 * Metrics SQL
 * %%
 * Copyright (C) 2014 Open-Source
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Start <code>Timer</code>s and increments <code>Counter</code>s
 * Internal helper class.
 */
class MetricHelper {
    private final MeterRegistry meterRegistry;
    private final MetricNamingStrategy metricNamingStrategy;
    private final ConcurrentHashMap<String, Timer> timerMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> counterMap = new ConcurrentHashMap<>();

    /**
     * Constructor
     * @param meterRegistry Registry storing metrics
     * @param metricNamingStrategy Strategy to name metrics
     */
    MetricHelper(MeterRegistry meterRegistry, MetricNamingStrategy metricNamingStrategy) {
        this.meterRegistry = meterRegistry;
        this.metricNamingStrategy = metricNamingStrategy;
    }

    private TimeObservation startTimer(String name) {
        if (name == null) {
            return null;
        }
        Timer timer = timerMap.computeIfAbsent(name, n -> Timer.builder(n).register(meterRegistry));
        return new TimeObservation(timer, System.nanoTime());
    }

    private void incCounter(String name) {
        if (name == null) {
            return;
        }
        Counter counter = counterMap.computeIfAbsent(name, n -> Counter.builder(n).register(meterRegistry));
        counter.increment();
    }

    public TimeObservation startConnectionLifeTimer() {
        return startTimer(metricNamingStrategy.getConnectionLifeTimer());
    }

    public TimeObservation startConnectionGetTimer() {
        return startTimer(metricNamingStrategy.getConnectionGetTimer());
    }

    /**
     * Start Timer when statement is created
     *
     * @return Started timer context or null
     */
    public TimeObservation startStatementLifeTimer() {
        return startTimer(metricNamingStrategy.getStatementLifeTimer());
    }
    /**
     * Start Timer when statement is executed
     *
     * @param query SQL query
     * @return Started timer context or null
     */
    public TimeObservation startStatementExecuteTimer(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getStatementExecuteTimer(query.getSql(), query.getSqlId());
        return startTimer(name);
    }

    private void ensureSqlId(Query query) {
        query.ensureSqlId(metricNamingStrategy);
    }

    /**
     * Start Timer when prepared statement is created
     *
     * @return Started timer context or null
     */
    public TimeObservation startPreparedStatementLifeTimer(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getPreparedStatementLifeTimer(query.getSql(), query.getSqlId());
        return startTimer(name);
    }

    /**
     * Start Timer when prepared statement is created
     *
     * @return Started timer context or null
     */
    public TimeObservation startPreparedStatementExecuteTimer(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getPreparedStatementExecuteTimer(query.getSql(), query.getSqlId());
        return startTimer(name);
    }

    private String getSqlId(String sqlId, String sql) {
        if (sqlId == null) {
            sqlId = metricNamingStrategy.getSqlId(sql);
        }
        return sqlId;
    }

    /**
     * Start Timer when callable statement is created
     *
     * @return Started timer context or null
     */
    public TimeObservation startCallableStatementLifeTimer(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getCallableStatementLifeTimer(query.getSql(), query.getSqlId());
        return startTimer(name);
    }

    /**
     * Start Timer when prepared statement is created
     *
     * @return Started timer context or null
     */
    public TimeObservation startCallableStatementExecuteTimer(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getCallableStatementExecuteTimer(query.getSql(), query.getSqlId());
        return startTimer(name);
    }

    /**
     * Start Timer when result set is created
     *
     * @return Started timer context or null
     */
    public TimeObservation startResultSetLifeTimer(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getResultSetLifeTimer(query.getSql(), query.getSqlId());
        return startTimer(name);
    }
    /**
     * Increment when result set row is read
     */
    public void markResultSetRowMeter(Query query) {
        ensureSqlId(query);
        String name = metricNamingStrategy.getResultSetRowMeter(query.getSql(), query.getSqlId());
        incCounter(name);
    }
}
