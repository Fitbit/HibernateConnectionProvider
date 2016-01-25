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
     * @see ConnectionProvider#closeConnection(Connection)
     */
    void recordConnectionAcquired();

    /**
     * Records a failed attempt to acquire a connection.
     * @param failureCause the exception that caused the failure
     * @see ConnectionProvider#closeConnection(Connection)
     */
    void recordAcquisitionFailure(Throwable failureCause);


    /**
     * Records how long a connection was held (time between getConnection and closeConnection) prior to being closed
     * successfully.
     * @param connectionUsageMillis the duration in milliseconds between acquiring the connection and closing it
     */
    void recordConnectionClosed(long connectionUsageMillis);

    /**
     * Records a failed attempt to close a connection.
     * @param connectionUsageMillis the duration in milliseconds between acquiring the connection and closing it
     * @param failureCause the exception that was thrown by <strong>closeConnection</strong>
     */
    void recordCloseFailure(long connectionUsageMillis, Throwable failureCause);
}
