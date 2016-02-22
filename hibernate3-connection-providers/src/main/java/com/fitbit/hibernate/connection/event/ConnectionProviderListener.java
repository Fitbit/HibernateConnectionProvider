package com.fitbit.hibernate.connection.event;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

import java.util.Properties;

/**
 * Interface abstraction around an interceptor that should be invoked immediately after acquiring new connections and
 * immediately before releasing used connections.
 *
 * @author dgarson
 */
public interface ConnectionProviderListener {

    /**
     * Initializes this interceptor given information about the ConnectionProvider and Hibernate configuration.
     *
     * @param settings listener &quot;settings&quot; and ConnectionProvider reference
     * @throws Exception on any exception initializing this listener
     * @see InstrumentedConnectionProvider#createListenerSettings(Properties)
     */
    void initialize(ConnectionProviderListenerSettings settings) throws Exception;
}
