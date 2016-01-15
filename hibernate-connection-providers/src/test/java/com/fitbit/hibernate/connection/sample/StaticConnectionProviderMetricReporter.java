package com.fitbit.hibernate.connection.sample;

/**
 * Dummy class that provides a way to report metrics collected in the provided
 * {@link ConnectionPerformanceMetricListener} without introducing another abstraction around metric reporting.
 *
 * @author dgarson
 */
public class StaticConnectionProviderMetricReporter {

    private static final StaticConnectionProviderMetricReporter INSTANCE = new StaticConnectionProviderMetricReporter();

    private long totalConnectionUsageMillis;
    private long totalConnectionAcquisitionTimeMillis;
    private long totalConnectionReleasingTimeMillis;
    private int numConnectionsAcquired;
    private int numAcquisitionFailures;
    private int numConnectionsReleased;

    public long getTotalConnectionAcquisitionTimeMillis() {
        return totalConnectionAcquisitionTimeMillis;
    }

    public long getTotalConnectionReleasingTimeMillis() {
        return totalConnectionReleasingTimeMillis;
    }

    public long getTotalConnectionUsageMillis() {
        return totalConnectionUsageMillis;
    }

    public int getNumConnectionsAcquired() {
        return numConnectionsAcquired;
    }

    public int getNumAcquisitionFailures() {
        return numAcquisitionFailures;
    }

    public int getNumConnectionsReleased() {
        return numConnectionsReleased;
    }

    public void recordRelease(long opDurationMillis, long connectionUsageMillis) {
        totalConnectionReleasingTimeMillis += opDurationMillis;
        totalConnectionUsageMillis += connectionUsageMillis;
        numConnectionsReleased++;
    }

    public void recordAcquisition(long opDurationMillis, boolean success) {
        if (success) {
            numConnectionsAcquired++;
        } else {
            numAcquisitionFailures++;
        }
        totalConnectionAcquisitionTimeMillis += opDurationMillis;
    }

    public void reset() {
        numConnectionsReleased = 0;
        numConnectionsAcquired = 0;
        numAcquisitionFailures = 0;
        totalConnectionUsageMillis = 0;
        totalConnectionReleasingTimeMillis = 0;
        totalConnectionAcquisitionTimeMillis = 0;
    }

    public static StaticConnectionProviderMetricReporter getInstance() {
        return INSTANCE;
    }
}
