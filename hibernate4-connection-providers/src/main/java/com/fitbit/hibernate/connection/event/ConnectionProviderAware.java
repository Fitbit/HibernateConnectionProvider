package com.fitbit.hibernate.connection.event;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

/**
 * Awareness interface that indicates that the ConnectionProviderListener needs to have its parent
 * InstrumentedConnectionProvider injected into it.
 *
 * @author dgarson
 */
public interface ConnectionProviderAware {

    /**
     * Injects the {@link com.fitbit.hibernate.connection.InstrumentedConnectionProvider} that is creating this
     * object.
     * @param connectionProvider the (parent) connection provider
     */
    void setConnectionProvider(InstrumentedConnectionProvider connectionProvider);
}
