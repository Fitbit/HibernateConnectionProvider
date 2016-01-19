package com.fitbit.hibernate.connection.impl;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;

import javax.annotation.Nonnull;

/**
 * Extension of the provided connection provider implementation that allows for injecting a custom Ticker such that
 * tests can use mock timings to avoid using actual {@link Thread#sleep(long)} calls.
 *
 * @author dgarson
 */
public class TestableInstrumentedConnectionProvider extends InstrumentedConnectionProvider {

    /**
     * Ticker that defaults to the system clock, which provides standard/expected behavior, but can be overridden in
     * test cases so that (real) sleeps are not necessary.
     */
    private Ticker ticker = Ticker.systemTicker();

    /**
     * Returns the ticker that should be used by the performance metric listener.
     */
    public @Nonnull Ticker getTicker() {
        return ticker;
    }

    /**
     * Sets the ticker used by this connection provider and the metric reporter.
     *
     */
    void setup(@Nonnull Ticker ticker, @Nonnull ConnectionMetricReporter connectionMetricReporter) {
        Preconditions.checkNotNull(ticker);
        this.ticker = ticker;

        // inject it manually into our ConnectionPerformanceMetricListener since it was already created
        ConnectionPerformanceMetricListener metricListener =
            getListenerOfType(ConnectionPerformanceMetricListener.class);
        Preconditions.checkState(metricListener != null, "Unable to find attached " +
            ConnectionPerformanceMetricListener.class);
        metricListener.setup(ticker, connectionMetricReporter);
    }
}
