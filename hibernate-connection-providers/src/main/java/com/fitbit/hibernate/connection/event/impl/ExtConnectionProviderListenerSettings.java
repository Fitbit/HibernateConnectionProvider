package com.fitbit.hibernate.connection.event.impl;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;
import com.fitbit.hibernate.connection.event.ConnectionProviderListenerSettings;

import com.google.common.base.Ticker;

import java.util.Properties;

import javax.annotation.Nonnull;

/**
 * Extension providing a {@link com.google.common.base.Ticker} so that the timings can be mocked in tests for the
 * sample metric listener implementation.
 *
 * @author dgarson
 */
public class ExtConnectionProviderListenerSettings extends ConnectionProviderListenerSettings {

    private final Ticker ticker;

    public ExtConnectionProviderListenerSettings(@Nonnull InstrumentedConnectionProvider connectionProvider,
                                                 @Nonnull Properties hibernateProperties,
                                                 @Nonnull Ticker ticker) {
        super(connectionProvider, hibernateProperties);
        this.ticker = ticker;
    }

    public Ticker getTicker() {
        return ticker;
    }
}
