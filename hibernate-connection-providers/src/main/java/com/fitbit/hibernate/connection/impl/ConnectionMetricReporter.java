package com.fitbit.hibernate.connection.impl;

import org.hibernate.connection.ConnectionProvider;

import java.sql.Connection;

/**
 * Basic contract for recording metrics that relate to the connection provider. <br/>
 * <strong>NOTE:</strong> Provided just for this sample and therefore was not written to be generally re-used.
 *
 * @author dgarson
 */
public interface ConnectionMetricReporter {

    /**
     * Records a successful acquisition of a Connection through the <strong>getConnection()</strong> method in the
     * connection provider.
     * @param acquisitionDurationMillis the time it took to call the <strong>getConnection()</strong> method.
     * @see ConnectionProvider#closeConnection(Connection)
     */
    void recordConnectionAcquired(long acquisitionDurationMillis);

    /**
     * Records a failed attempt to acquire a connection. Even though the connection acquisition failed, it may have
     * taken some time, so the time it took is provided here as well.
     * @param failedAcquisitionDurationMillis the time it took executing the <strong>getConnection()</strong> method
     *          prior to failing
     * @param failureCause the exception that caused the failure
     * @see ConnectionProvider#closeConnection(Connection)
     */
    void recordAcquisitionFailure(long failedAcquisitionDurationMillis, Throwable failureCause);


    /**
     * Records how long a connection was held (time between getConnection and closeConnection) prior to being closed
     * successfully, as well as the time it took to actually execute the <strong>closeConnection</strong> operation.
     * <br/>
     * The time it took takes to execute the <strong>closeConnection</strong> close the connection is often
     * instantaneous but is entirely implementation-dependent, so it is provided here.
     * @param closeConnectionDurationMillis the time it took to call the
     * {@link org.hibernate.connection.ConnectionProvider#closeConnection(Connection)} method, in milliseconds
     * @param connectionUsageMillis the duration in milliseconds between acquiring the connection and closing it
     */
    void recordConnectionClosed(long closeConnectionDurationMillis, long connectionUsageMillis);

    /**
     * Records a failed attempt to close a connection. Both the time it took executing
     * {@link ConnectionProvider#closeConnection(Connection)} (before it failed) along with the duration the that the
     * connection had been held at that time.
     * @param failedCloseConnectionDurationMillis the time it took to call the
     * {@link org.hibernate.connection.ConnectionProvider#closeConnection(Connection)} method, in milliseconds
     * @param connectionUsageMillis the duration in milliseconds between acquiring the connection and closing it
     * @param failureCause the exception that was thrown by <strong>closeConnection</strong>
     */
    void recordCloseFailure(long failedCloseConnectionDurationMillis, long connectionUsageMillis,
                            Throwable failureCause);
}
