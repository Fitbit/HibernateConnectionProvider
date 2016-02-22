package com.fitbit.hibernate.connection.event;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

/**
 * Listener type that is invoked immediately prior to acquiring a new connection from the underlying connection provider
 * implementation (e.g. c3p0).
 *
 * @author dgarson
 */
public interface PreConnectionAcquisitionListener extends ConnectionProviderListener {

    /**
     * Callback invoked immediately prior to attempting to acquire a connection.
     *
     * @param connectionProvider the connection provider being used to acquire the new connection
     */
    void beforeConnectionAcquisition(InstrumentedConnectionProvider connectionProvider);
}
