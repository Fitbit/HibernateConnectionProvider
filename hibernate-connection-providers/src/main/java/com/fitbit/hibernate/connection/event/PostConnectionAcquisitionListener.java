package com.fitbit.hibernate.connection.event;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

import java.sql.Connection;

/**
 * Listener type that listens only to post-connection acquisition events, which includes both successful acquisitions
 * and failures.
 *
 * @author dgarson
 */
public interface PostConnectionAcquisitionListener extends ConnectionProviderListener {

    /**
     * Callback invoked immediately after <strong>successfully</strong> acquiring a connection.
     *
     * @param connectionProvider the connection provider invoking this callback
     * @param connection the acquired connection
     */
    void afterConnectionAcquired(InstrumentedConnectionProvider connectionProvider, Connection connection);

    /**
     * Callback invoked when an exception occurs (<strong>failure</strong>) during connection acquisition.
     *
     * @param connectionProvider the connection provider invoking this callback
     * @param exc the exception thrown
     */
    void afterConnectionAcquisitionFailed(InstrumentedConnectionProvider connectionProvider, Throwable exc);
}
