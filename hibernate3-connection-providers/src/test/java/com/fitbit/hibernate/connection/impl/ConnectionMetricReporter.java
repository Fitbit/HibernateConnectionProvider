package com.fitbit.hibernate.connection.impl;

import org.hibernate.connection.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Class that provides a basic way to report metrics collected in the provided
 * {@link ConnectionPerformanceMetricListener} without introducing another abstraction around metric reporting.
 *
 * @author dgarson
 */
public class ConnectionMetricReporter {

    private static final Logger log = LoggerFactory.getLogger(ConnectionMetricReporter.class);

    private static final ConnectionMetricReporter INSTANCE = new ConnectionMetricReporter();

    /** the total elapsed time that all acquired connections (that are also closed) were held before being closed */
    private long totalConnectionUsageMillis;
    /** the number of successfully acquired connections */
    private int numTopLevelAcquired;
    private int totalNumAcquired;
    /** the number of {@link ConnectionProvider#getConnection()} calls that failed */
    private int numAcquisitionFailures;
    /** the number of successfully closed connections */
    private int numTopLevelClosed;
    private int totalNumClosed;
    /** the number of {@link ConnectionProvider#closeConnection(Connection)} calls that failed */
    private int numCloseConnectionFailures;
    /** the total number of operations recorded, regardless of success/failure */
    private int totalNumOps;

    public long getTotalConnectionUsageMillis() {
        return totalConnectionUsageMillis;
    }

    public int getNumTopLevelAcquired() {
        return numTopLevelAcquired;
    }

    public int getTotalNumAcquired() {
        return totalNumAcquired;
    }

    public int getNumAcquisitionFailures() {
        return numAcquisitionFailures;
    }

    public int getNumTopLevelClosed() {
        return numTopLevelClosed;
    }

    public int getTotalNumClosed() {
        return totalNumClosed;
    }

    public int getNumCloseConnectionFailures() {
        return numCloseConnectionFailures;
    }

    public int getTotalNumOps() {
        return totalNumOps;
    }

    public void recordAcquisitionFailure(boolean isTopLevel, Throwable failureCause) {
        log.error("Failed to acquire a connection", failureCause);
        numAcquisitionFailures++;
        totalNumOps++;
    }

    public void recordConnectionAcquired(boolean isTopLevel) {
        log.info("Connection successfully acquired");
        // only top-level connections have timing information and only they should be counted here
        if (isTopLevel) {
            numTopLevelAcquired++;
        }
        totalNumAcquired++;
        totalNumOps++;
    }

    public void recordConnectionClosed(boolean isTopLevel, long connectionUsageMillis) {
        // we will not have a valid timing if we are not talking about a 'top-level' connection
        if (isTopLevel) {
            totalConnectionUsageMillis += connectionUsageMillis;
            numTopLevelClosed++;
            log.info("Successfully closed a connection that was held open for {}ms", connectionUsageMillis);
        } else {
            log.info("Successfully closed a connection");
        }
        totalNumClosed++;
        totalNumOps++;
    }

    public void reset() {
        numTopLevelAcquired = 0;
        totalNumAcquired = 0;
        numTopLevelClosed = 0;
        totalNumClosed = 0;
        numAcquisitionFailures = 0;
        numCloseConnectionFailures = 0;
        totalConnectionUsageMillis = 0;
        totalNumOps = 0;
    }

    public static ConnectionMetricReporter getInstance() {
        return INSTANCE;
    }
}
