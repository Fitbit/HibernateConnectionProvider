package com.fitbit.hibernate.connection.event;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Object that encapsulates settings/referenced objects used by implementations of {@link ConnectionProviderListener}.
 * <br/>
 * This class should be subclassed if additional properties are desired by listener implementations. See the factory
 * method in the ConnectionProviderListener class.
 *
 * @author dgarson
 */
public class ConnectionProviderListenerSettings {

    private final InstrumentedConnectionProvider connectionProvider;
    private final Properties hibernateProperties;

    public ConnectionProviderListenerSettings(@Nonnull InstrumentedConnectionProvider connectionProvider,
                                              @Nonnull Properties hibernateProperties) {
        this.connectionProvider = connectionProvider;
        this.hibernateProperties = hibernateProperties;
    }

    /**
     * Returns the JDBC url with which the ConnectionProvider was configured.
     */
    public @Nonnull String getJdbcUrl() {
        return connectionProvider.getJdbcUrl();
    }

    /**
     * Returns the connection provider to which the listener is being attached.
     */
    public @Nonnull InstrumentedConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    /**
     * Returns the Hibernate properties used to configure the parent ConnectionProvider.
     */
    public @Nonnull Properties getHibernateProperties() {
        return hibernateProperties;
    }

    /**
     * Gets a Hibernate property with an optional default value.
     *
     * @see Properties#getProperty(String, String)
     * @see #getHibernateProperties()
     */
    public @Nullable String getProperty(@Nonnull String propertyName, @Nullable String defaultValue) {
        return hibernateProperties.getProperty(propertyName, defaultValue);
    }

    /**
     * Gets a Hibernate property for a given name, returning <code>null</code> if that property does not exist.
     *
     * @see Properties#getProperty(String)
     * @see #getHibernateProperties()
     */
    public @Nullable String getProperty(@Nonnull String propertyName) {
        return hibernateProperties.getProperty(propertyName);
    }

    /**
     * Gets a Hibernate property for a given name and converts it to a boolean value, returning the
     * <strong>defaultValue</strong> if the property is not defined or is not a valid boolean value as defined by
     * {@link BooleanUtils#toBoolean(String)}. All values are case-insensitive. <br/>
     * Supports true values of:
     * <ul>
     *     <li>true</li>
     *     <li>t</li>
     *     <li>yes</li>
     *     <li>y</li>
     *     <li>on</li>
     * </ul>
     * and false values of:
     * <ul>
     *     <li>false</li>
     *     <li>f</li>
     *     <li>no</li>
     *     <li>n</li>
     *     <li>off</li>
     * </ul>
     *
     * @param propertyName the property name
     * @param defaultValue the default value to return
     */
    public boolean getBooleanProperty(@Nonnull String propertyName, boolean defaultValue) {
        String value = hibernateProperties.getProperty(propertyName);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        Boolean parsedVal = BooleanUtils.toBooleanObject(value);
        return (parsedVal != null ? parsedVal : defaultValue);
    }
}