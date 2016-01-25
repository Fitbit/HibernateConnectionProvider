package com.fitbit.hibernate.connection.impl;

import org.hibernate.connection.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Dummy class that provides a way to report metrics collected in the provided
 * {@link ConnectionPerformanceMetricListener} without introducing another abstraction around metric reporting.
 *
 * @author dgarson
 */
public class StaticConnectionProviderMetricReporter implements ConnectionMetricReporter {

    private static final Logger log = LoggerFactory.getLogger(StaticConnectionProviderMetricReporter.class);

    private static final StaticConnectionProviderMetricReporter INSTANCE = new StaticConnectionProviderMetricReporter();

    /** the total elapsed time that all acquired connections (that are also closed) were held before being closed */
    private long totalConnectionUsageMillis;
    /** the number of successfully acquired connections */
    private int numConnectionsAcquired;
    /** the number of {@link ConnectionProvider#getConnection()} calls that failed */
    private int numAcquisitionFailures;
    /** the number of successfully closed connections */
    private int numConnectionsClosed;
    /** the number of {@link ConnectionProvider#closeConnection(Connection)} calls that failed */
    private int numCloseConnectionFailures;
    /** the total number of operations recorded, regardless of success/failure */
    private int totalNumOps;

    public long getTotalConnectionUsageMillis() {
        return totalConnectionUsageMillis;
    }

    public int getNumConnectionsAcquired() {
        return numConnectionsAcquired;
    }

    public int getNumAcquisitionFailures() {
        return numAcquisitionFailures;
    }

    public int getNumConnectionsClosed() {
        return numConnectionsClosed;
    }

    public int getNumCloseConnectionFailures() {
        return numCloseConnectionFailures;
    }

    public int getTotalNumOps() {
        return totalNumOps;
    }

    @Override
    public void recordAcquisitionFailure(Throwable failureCause) {
        log.error("Failed to acquire a connection", failureCause);
        numAcquisitionFailures++;
        totalNumOps++;
    }

    @Override
    public void recordConnectionAcquired() {
        log.info("Connection successfully acquired");
        numConnectionsAcquired++;
        totalNumOps++;
    }

    @Override
    public void recordConnectionClosed(long connectionUsageMillis) {
        log.info("Successfully closed a connection that was held open for {}ms", connectionUsageMillis);
        numConnectionsClosed++;
        totalConnectionUsageMillis += connectionUsageMillis;
        totalNumOps++;
    }

    @Override
    public void recordCloseFailure(long connectionUsageMillis, Throwable failureCause) {
        log.error("Failed to close a connection that was held open for {}ms", connectionUsageMillis, failureCause);
        numCloseConnectionFailures++;
        totalConnectionUsageMillis += connectionUsageMillis;
        totalNumOps++;
    }

    public void reset() {
        numConnectionsClosed = 0;
        numConnectionsAcquired = 0;
        numAcquisitionFailures = 0;
        numCloseConnectionFailures = 0;
        totalConnectionUsageMillis = 0;
    }

    public static StaticConnectionProviderMetricReporter getInstance() {
        return INSTANCE;
    }
}
