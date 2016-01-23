package com.fitbit.hibernate.connection.impl;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;
import com.fitbit.hibernate.connection.event.ConnectionProviderListenerSettings;
import com.fitbit.hibernate.connection.event.PostConnectionAcquisitionListener;
import com.fitbit.hibernate.connection.event.PostConnectionCloseListener;
import com.fitbit.hibernate.connection.event.PreConnectionAcquisitionListener;
import com.fitbit.hibernate.connection.event.PreConnectionCloseListener;
import com.fitbit.util.ThreadLocalCounter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import org.hibernate.connection.ConnectionProvider;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

/**
 * Sample implementation of a ConnectionProviderListener that adds support for timing connection provider operations and
 * recording those metrics somewhere. <br/>
 * This class uses an abstraction for recording metrics since the mechanism through which metrics are recorded often
 * varies between applications and environments.
 *
 * @author dgarson
 */
public class ConnectionPerformanceMetricListener implements PreConnectionAcquisitionListener,
    PostConnectionAcquisitionListener, PreConnectionCloseListener, PostConnectionCloseListener {

    /**
     * The ticker to use for stopwatches in this class.
     */
    private Ticker ticker;

    /**
     * Object capable of recording values for the metrics measured by this listener
     */
    private ConnectionMetricReporter connectionMetricReporter;

    /**
     * Thread-local stopwatch that is used to measure the time a connection is held for when acquired from the owning
     * provider prior to being closed/released.
     */
    private final ThreadLocal<Stopwatch> connectionUsageStopwatch = new ThreadLocal<Stopwatch>() {
        @Override
        protected Stopwatch initialValue() {
            // use the ticker that was provided so we can mock timings
            return Stopwatch.createUnstarted(ticker);
        }
    };

    /**
     * Thread-local stopwatch that is used to measure how much time it takes for a consumer to acquire or release a
     * connection from/to this provider. This measures the time between entering and exiting either the
     * {@link ConnectionProvider#getConnection()} or {@link ConnectionProvider#closeConnection(Connection)} methods.
     */
    private final ThreadLocal<Stopwatch> operationTimingStopwatch = new ThreadLocal<Stopwatch>() {
        @Override
        protected Stopwatch initialValue() {
            // use the ticker that was provided so we can mock timings
            return Stopwatch.createUnstarted(ticker);
        }
    };

    /**
     * Keeps track of the depth in connection acquisition calls to the specific ConnectionProvider this listener is
     * attached to. Since we are using thread-locals here, we cannot support nested connection acquisitions, and this
     * counter ensures that we do not attempt to do so.
     */
    private final ThreadLocalCounter connectionAcquisitionDepth = new ThreadLocalCounter();

    @Override
    public void initialize(ConnectionProviderListenerSettings settings) throws Exception {
        // we would otherwise grab the Ticker here but we must set the ticker after ConnectionProvider configuration
        //  so we are unable to use the mock ticker at this point.
    }

    @VisibleForTesting
    void setup(@Nonnull Ticker ticker, @Nonnull ConnectionMetricReporter connectionMetricReporter) {
        Preconditions.checkNotNull(ticker);
        Preconditions.checkNotNull(connectionMetricReporter);
        this.ticker = ticker;
        this.connectionMetricReporter = connectionMetricReporter;
    }

    @Override
    public void beforeConnectionAcquisition(InstrumentedConnectionProvider connectionProvider) {
        // restart the stopwatch for the time it takes to acquire a connection
        operationTimingStopwatch.get().reset().start();
    }

    @Override
    public void afterConnectionAcquired(InstrumentedConnectionProvider connectionProvider, Connection connection) {
        // just record the time it took to successfully acquire this connection
        long elapsedMillis = operationTimingStopwatch.get().stop().elapsed(TimeUnit.MILLISECONDS);
        connectionMetricReporter.recordConnectionAcquired(elapsedMillis);

        // reset and start the usage stopwatch now if this is the only connection that will be checked out from this
        //      provider (e.g. there is not already one that has been acquired but *not* released)
        int depth = connectionAcquisitionDepth.incrementAndGet();
        if (depth == 1) {
            connectionUsageStopwatch.get().reset().start();
        }
    }

    @Override
    public void afterConnectionAcquisitionFailed(InstrumentedConnectionProvider connectionProvider, Throwable exc) {
        // just record the time it took to try and fail to acquire this connection
        long elapsedMillis = operationTimingStopwatch.get().stop().elapsed(TimeUnit.MILLISECONDS);
        connectionMetricReporter.recordAcquisitionFailure(elapsedMillis, exc);
    }

    @Override
    public void beforeClosingConnection(Connection connection) {
        int depth = connectionAcquisitionDepth.decrementAndGet();
        // make sure we haven't released a connection more times than we have acquired one
        if (depth < 0) {
            throw new IllegalStateException("getConnection and closeConnection calls are not balanced. Is a " +
                "connection being closed multiple times?");
        } else if (depth == 0) {
            // this marks the last time the user code held the connection, so we can halt the usage duration stopwatch
            //      but wait until after the operation completes to record it since we do not yet know how long it took
            //      to perform the actual release
            connectionUsageStopwatch.get().stop();
        }


        // start the stopwatch here since we are about to release a connection and can measure the time it takes
        operationTimingStopwatch.get().reset().start();
    }

    @Override
    public void afterConnectionClosed() {
        // this stopwatch has already been stopped in beforeClosingConnection(..)
        long usageDurationMillis = connectionUsageStopwatch.get().elapsed(TimeUnit.MILLISECONDS);
        long opDurationMillis = operationTimingStopwatch.get().stop().elapsed(TimeUnit.MILLISECONDS);
        connectionMetricReporter.recordConnectionClosed(opDurationMillis, usageDurationMillis);
    }

    @Override
    public void afterConnectionClosingFailed(Connection connection, Throwable exc) {
        // this stopwatch has already been stopped in beforeClosingConnection(..)
        long usageDurationMillis = connectionUsageStopwatch.get().elapsed(TimeUnit.MILLISECONDS);
        long opDurationMillis = operationTimingStopwatch.get().stop().elapsed(TimeUnit.MILLISECONDS);
        connectionMetricReporter.recordCloseFailure(opDurationMillis, usageDurationMillis, exc);
    }
}
