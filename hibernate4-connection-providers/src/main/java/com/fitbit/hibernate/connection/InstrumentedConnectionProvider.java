package com.fitbit.hibernate.connection;

import com.fitbit.hibernate.connection.event.ConnectionProviderAware;
import com.fitbit.hibernate.connection.event.ConnectionProviderListener;
import com.fitbit.hibernate.connection.event.ConnectionProviderListenerSettings;
import com.fitbit.hibernate.connection.event.PostConnectionAcquisitionListener;
import com.fitbit.hibernate.connection.event.PostConnectionCloseListener;
import com.fitbit.hibernate.connection.event.PreConnectionAcquisitionListener;
import com.fitbit.hibernate.connection.event.PreConnectionCloseListener;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.internal.ConnectionProviderInitiator;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * ConnectionProvider implementation that can be used to wrap another provider such that listeners and any associated
 * instrumentation can be applied on the ConnectionProvider level. <br/>
 * Be sure to include the delegating provider class name along with the listener class names when configuring this
 * connection provider.
 *
 * @author dgarson
 */
public class InstrumentedConnectionProvider implements ConnectionProvider, Configurable, Stoppable,
    ServiceRegistryAwareService {

    public static final String DELEGATE_CONNECTION_PROVIDER_CLASS = "hibernate.connection.delegate_provider_class";
    public static final String CONNECTION_PROVIDER_LISTENERS = "hibernate.connection.provider_listener_classes";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private ServiceRegistryImplementor serviceRegistry;

    // the real connection provider implementation that this provider is delegating operations to
    private ConnectionProvider delegateConnectionProvider;

    // cached JDBC connection URL
    private String jdbcUrl;

    // list of zero or more ConnectionProviderListener that are registered with this connection provider wrapper
    private final List<PreConnectionAcquisitionListener> preAcquisitionListeners = new ArrayList<>();
    private final List<PostConnectionAcquisitionListener> postAcquisitionListeners = new ArrayList<>();
    private final List<PreConnectionCloseListener> preCloseListeners = new ArrayList<>();
    private final List<PostConnectionCloseListener> postCloseListeners = new ArrayList<>();
    private final Map<Class<? extends ConnectionProviderListener>, ConnectionProviderListener> listenerMap =
        new HashMap<>();

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    /**
     * Configures this instrumented provider using the provided Hibernate properties, most of which are just passed
     * through to the delegate (&quot;actual&quot;) connection provider implementation which must be provided in the
     * &quot;hibernate.connection.delegate_provider_class&quot; Hibernate property. This method will invoke callbacks in
     * this class before and after creating the delegate provider using the {@link ConnectionProviderInitiator}, then
     * creates and configures all {@link ConnectionProviderListener} whose classes are listed in the corresponding
     * Hibernate property: &quot;hibernate.connection.connection_provider_listeners&quot.
     * @param configurationValues the Hibernate properties
     * @throws HibernateException any exceptions thrown during configuration
     * @see ConnectionProviderInitiator#initiateService(Map, ServiceRegistryImplementor)
     */
    @Override
    public final void configure(Map configurationValues) throws HibernateException {
        // allow subclass to pre-process if desired
        preConfigure(configurationValues, serviceRegistry);

        // make sure we have a service registry
        Preconditions.checkState(serviceRegistry != null, "missing ServiceRegistry for Hibernate DI");

        // cache JDBC url
        jdbcUrl = (String) configurationValues.get(Environment.URL);

        // create the real connection provider instance
        //      restore the real connection provider class name from the delegating property name
        String delegateConnectionProviderClass = (String) configurationValues.get(DELEGATE_CONNECTION_PROVIDER_CLASS);

        // make sure to swap in the delegate for the provider property
        Map<Object, Object> delegateConfigValues = new HashMap<>(configurationValues);
        if (StringUtils.isNotBlank(delegateConnectionProviderClass)) {
            delegateConfigValues.put(Environment.CONNECTION_PROVIDER, delegateConnectionProviderClass);
        } else {
            // allow Hibernate to use its own mechanism for determining the proper built-in provider to use
            log.warn("No explicit '{}' was declared so the default for the JDBC URL will be used.",
                DELEGATE_CONNECTION_PROVIDER_CLASS);
            delegateConfigValues.remove(Environment.CONNECTION_PROVIDER);
        }

        delegateConfigValues.put(Environment.CONNECTION_PROVIDER, delegateConnectionProviderClass);
        delegateConnectionProvider =
            ConnectionProviderInitiator.INSTANCE.initiateService(delegateConfigValues, serviceRegistry);
        log.trace("Created delegate connection provider of type {} connected to {}",
            delegateConnectionProvider.getClass(), jdbcUrl);

        // do actual initialization in subclass impl.
        initialize(configurationValues);

        // configure our connection provider lists
        configureListeners(configurationValues);

        // post-configuration callback
        configured(configurationValues);
        log.info("Finished initializing InstrumentedConnectionProvider for URL: {}", jdbcUrl);
    }

    /**
     * Invoked immediately prior to the business logic in {@link #configure(Map)} method.
     * @param configurationValues the configuration values
     * @param serviceRegistry the service registry that can be used to locate hibernate services
     */
    protected void preConfigure(@Nonnull Map configurationValues, @Nonnull ServiceRegistryImplementor serviceRegistry) {
        // no-op
    }

    /**
     * Callback that can be overridden by a subclass and will be invoked by {@link #configure(Map)} so that
     * any additional initialization logic can be performed.
     */
    protected void initialize(@Nonnull Map configurationValues) throws HibernateException {
        // no-op
    }

    /**
     * Callback method invoked at the end of the {@link #configure(Map)} call so that post-processing can be
     * done.
     * @param configurationValues the properties used to configure this connection provider
     */
    protected void configured(Map configurationValues){
        // no-op by default, optional override
    }

    @Override
    public boolean isUnwrappableAs(Class unwrapType) {
        return ConnectionProvider.class.equals(unwrapType) ||
            InstrumentedConnectionProvider.class.equals(unwrapType) ||
                delegateConnectionProvider.isUnwrappableAs(unwrapType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> unwrapType) {
        if (ConnectionProvider.class.equals(unwrapType)) {
            return (T) delegateConnectionProvider;
        } else if (unwrapType.isInstance(delegateConnectionProvider)) {
            return unwrapType.cast(delegateConnectionProvider);
        } else if (delegateConnectionProvider.isUnwrappableAs(unwrapType)) {
            return delegateConnectionProvider.unwrap(unwrapType);
        } else {
            throw new IllegalArgumentException("Unable to unwrap to " + unwrapType);
        }
    }

    /**
     * Creates a new {@link com.fitbit.hibernate.connection.event.ConnectionProviderListenerSettings} object that will
     * be passed into listeners being attached to this connection provider.
     * @param hibernateProperties the hibernate properties provided here will also be automatically injected by this
     *              connection provider after this method returns a new settings object
     */
    protected
    @Nonnull ConnectionProviderListenerSettings createListenerSettings(Map<Object, Object> hibernateProperties) {
        return new ConnectionProviderListenerSettings(this, hibernateProperties);
    }

    /**
     * Callback that is invoked after prior to calling all post-acquisition callbacks for failures. This provides
     * subclasses the opportunity to either squelch or customize reactions to and respond to exceptions acquiring
     * connections outside of the listeners attached to this provider. </br>
     * After this method returns, the SQLException will be propagated up, unless another {@link SQLException} is
     * returned from this call.
     */
    protected @Nonnull SQLException handleAcquisitionFailure(@Nonnull SQLException exception) {
        return exception;
    }

    /**
     * Callback that is invoked at the very beginning of {@link #getConnection()}, prior to invoking any registered
     * listeners, allowing a subclass to prepare any state/context, such as any that might be needed by a listener or
     * other related components.
     */
    protected void beforeAcquiringConnection() {
        // optional override
    }

    /**
     * Callback that is invoked after acquiring a connection through the {@link #getConnection()} method but prior to
     * invoking any of the registered post-acquisition callbacks. This allows the connection provider to
     * @param connection the exact connection instance that was acquired from the underlying provider
     */
    protected void connectionAcquired(@Nonnull Connection connection) {
        // optional override
    }

    /**
     * Callback that is invoked immediately prior to calling {@link ConnectionProvider#closeConnection(Connection)} for
     * the provided <strong>connection</strong>.
     */
    protected void beforeClosingConnection(@Nonnull Connection connection) {
        // optional override
    }

    /**
     * Callback that is invoked immediately after calling {@link ConnectionProvider#closeConnection(Connection)} for
     * the provided <strong>connection</strong>. This method is gauranteed to execute <strong>after</strong> all
     * listeners have been invoked. This method should only be used if a subclass wishes to hook into the close
     * operation, in which case it should not be forced to register with itself as a listener :-)
     */
    protected void afterClosingConnection(@Nonnull Connection connection) {
        // optional override
    }

    /**
     * Callback that is invoked immediately when an attempt to call {@link #closeConnection(Connection)} fails, but
     * prior to any event listener callbacks being invoked.
     * @param connection the connection attempted to close
     * @param exception the exception thrown during that operation
     */
    protected void afterCloseConnectionFailed(@Nonnull Connection connection, Exception exception) {
        // optional override
    }

    /**
     * &quot;Customizes&quot; a connection, performing any operations, such as book-keeping, prior to invoking any
     * listeners after a connection is acquired.
     */
    protected void customizeConnection(@Nonnull Connection connection) {
        // optional override
    }

    /**
     * Callback that is invoked prior to performing the actual {@link #stop()} on the delegate
     * connection provider, allowing a subclass to hook into this event. This will be invoked even if the delegate
     * does not implement {@link Stoppable} as a lifecycle callback.
     */
    protected void beforeStop() {
        // optional override
    }

    /**
     * Looks up an existing, registered connection provider listener of the given type. If no listener is registered of
     * that type then this method will return <code>null</code>.
     */
    protected <T extends ConnectionProviderListener> T getListenerOfType(@Nonnull Class<T> listenerType) {
        return (T) listenerMap.get(listenerType);
    }

    /**
     * Returns the real connection provider that this instrumented wrapper is delegating to. This may return a value of
     * <code>null</code> if the {@link #configure(Map)} method has not yet been invoked.
     */
    @Nullable
    public ConnectionProvider getWrappedConnectionProvider() {
        return delegateConnectionProvider;
    }

    /**
     * Returns the JDBC url that this connection provider will be using for establishing connections.
     */
    @Nonnull
    public final String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public void stop() {
        try {
            beforeStop();
        } finally {
            if (delegateConnectionProvider instanceof Stoppable) {
                ((Stoppable)delegateConnectionProvider).stop();
            }
        }
    }

    @Override
    public boolean supportsAggressiveRelease() {
        return delegateConnectionProvider.supportsAggressiveRelease();
    }

    @Override
    public final Connection getConnection() throws SQLException {
        // allow connection provider to prepare any state needed for listeners that might be subscribed to it, or
        //      any other book-keeping, etc.
        beforeAcquiringConnection();

        // invoke pre-acquisition callbacks
        invokeListeners(/* isBefore=*/ true, /*isAcquisition=*/true, /*exc=*/null, /*connection=*/null);

        Connection acquiredConn;
        try {
            // acquire a connection using the delegate connection provider
            acquiredConn = delegateConnectionProvider.getConnection();

            // invoke internal callbacks before registered listeners are called
            afterAcquireBeforeCallbacks(acquiredConn);
        } catch (SQLException se) {
            // we are catching these to make sure we can properly invoke the failure listeners that are attached to this
            //      operation. if we immediately propagated the exception up to the caller, then we would never invoke
            //      these listeners

            // notify subclass of failure prior to invoking failure listeners and allow it to throw a new exception
            SQLException exceptionToUse = handleAcquisitionFailure(se);
            if (exceptionToUse == null) {
                // use the thrown exception
                log.warn("handleAcquisitionFailure returned null");
                exceptionToUse = se;
            }

            // invoke failure listeners
            invokeListeners(/*isBefore=*/false, /*isAcquisition=*/true, /*exception=*/exceptionToUse,
                /*connection=*/null);

            // rethrow the exception
            throw exceptionToUse;
        } catch (RuntimeException e) {
            // invoke failure listeners
            invokeListeners(/*isBefore=*/false, /*isAcquisition=*/true, /*exception=*/e,
                /*connection=*/null);
            // rethrow the exception
            throw e;
        }

        // invoke the listeners post-acquisition
        invokeListeners(/*isBefore=*/false, /*isAcquisition=*/true, /*exception=*/null, /*connection=*/acquiredConn);

        // invoke callback for subclass after listeners
        connectionAcquired(acquiredConn);

        return acquiredConn;
    }

    @Override
    public final void closeConnection(Connection existingConn) throws SQLException {
        // invoke pre-* callbacks
        invokeListeners(/* isBefore=*/ true, /*isAcquisition=*/false, /*exc=*/null,
            /*connection=*/existingConn);

        try {
            beforeClosingConnection(existingConn);

            // simply close the connection and return null
            delegateConnectionProvider.closeConnection(existingConn);
        } catch (SQLException | RuntimeException e) {
            // allow subclass to handle prior to invoking event listener callbacks
            afterCloseConnectionFailed(existingConn, e);

            // we are catching these to make sure we can properly invoke the failure listeners that are attached to this
            //      operation. if we immediately propagated the exception up to the caller, then we would never invoke
            //      these listeners
            // make sure to pass in the 'existingConnection' value
            invokeListeners(/* isBefore=*/ false, /*isAcquisition=*/false, /*exc=*/e, /*connection=*/existingConn);

            // rethrow the exception
            throw e;
        }

        // if we get this far, then we succeeded at applying the operation and must invoke post-operation callbacks
        //          on interceptors
        invokeListeners(/* isBefore=*/ false, /*isAcquisition=*/false, /*exc=*/null, /*connection=*/existingConn);

        // invoke callback in subclass
        afterClosingConnection(existingConn);
    }

    /**
     * Apply customization logic, if applicable. We must always do this before invoking our listener callbacks since the
     * such actions may be a prerequisite for a listener callback.
     * @param acquiredConn the acquired connection
     * @see #customizeConnection(Connection)
     */
    private void afterAcquireBeforeCallbacks(Connection acquiredConn) {
        if (acquiredConn != null) {
            customizeConnection(acquiredConn);
        }
    }

    /**
     * Invokes all subscribed interceptor callbacks for the specified operation.
     * @param isBefore true if invoking a pre vs. post listener handler method
     * @param isAcquisition true if acquiring a connection, false if releasing a connection
     * @param exception any exception that was caught while performing the operation requested
     * @param connection any connection that was being closed (on exception) or an opened connection (after successful
     *                  acquire), or <code>null</code> after a successful release and failed acquire
     */
    private void invokeListeners(boolean isBefore, boolean isAcquisition, Throwable exception, Connection connection) {
        for (ConnectionProviderListener listener :
                /* only loop over the listeners that are registered for this particular event type (pre/post open/close) */
            getListenersForEventType(isBefore, isAcquisition)) {
            // allow the listener itself to determine whether it is enabled and will have this callback invocation skipped or not
            String methodName = "<unknown>";
            try {
                if (isAcquisition) {
                    if (isBefore) {
                        methodName = "beforeConnectionAcquisition";
                        ((PreConnectionAcquisitionListener) listener).beforeConnectionAcquisition(this);
                    } else {
                        PostConnectionAcquisitionListener postListener = (PostConnectionAcquisitionListener) listener;
                        if (exception != null) {
                            methodName = "afterConnectionAcquisitionFailed";
                            postListener.afterConnectionAcquisitionFailed(this, exception);
                        } else {
                            methodName = "afterConnectionAcquired";
                            postListener.afterConnectionAcquired(this, connection);
                        }
                    }
                } else {
                    if (isBefore) {
                        methodName = "beforeClosingConnection";
                        ((PreConnectionCloseListener) listener).beforeClosingConnection(connection);
                    } else {
                        PostConnectionCloseListener postListener = (PostConnectionCloseListener) listener;
                        if (exception != null) {
                            methodName = "afterConnectionClosingFailed";
                            postListener.afterConnectionClosingFailed(connection, exception);
                        } else {
                            methodName = "afterConnectionClosed";
                            postListener.afterConnectionClosed();
                        }
                    }
                }
            } catch (Throwable t) {
                if (log.isErrorEnabled()) {
                    log.error("Unable to invoke {} for listener of type '{}' connected to {}", methodName,
                        listener.getClass(), jdbcUrl, t);
                }
            }
        }
    }

    private List<? extends ConnectionProviderListener> getListenersForEventType(boolean isBefore, boolean isAcquisition) {
        if (isAcquisition) {
            return (isBefore ? preAcquisitionListeners : postAcquisitionListeners);
        } else {
            return (isBefore ? preCloseListeners : postCloseListeners);
        }
    }

    /**
     * Creates and attaches any listeners that are declared for the connection provider in the Hibernate properties.
     * This method is called from the {@link #configure(Map)} method.
     * @param configurationValues the hibernate properties
     */
    private void configureListeners(Map<Object, Object> configurationValues) {
        // cleanup any possible listeners that were registered already, which will only be the case in tests
        removeAllListeners();

        // grab listener classes from the data source configuration
        // look for listener definitions, defaults to a zero-token array of class names
        String strListenerClasses = (String) configurationValues.get(CONNECTION_PROVIDER_LISTENERS);
        String[] listenerClasses = strListenerClasses != null ? StringUtils.split(strListenerClasses, ",") : null;
        int numListeners = 0;
        // reset any previously instantiated listeners in case we are using this method multiple times (such as in a
        // test)
        removeAllListeners();
        // loop through configured listeners and create/attach them
        if (listenerClasses != null) {
            for (String listenerClassName : listenerClasses) {
                try {
                    Class<? extends ConnectionProviderListener> listenerClazz = Class.forName(
                        listenerClassName).asSubclass(
                        ConnectionProviderListener.class);
                    ConnectionProviderListener listener = createListener(configurationValues, listenerClazz);
                    // once we've created and initialized the listener, add it the appropriate listener lists
                    addToListenerLists(listener);
                    numListeners++;
                } catch (Exception e) {
                    log.error("Unable to attach {} to InstrumentedConnectionProvider for {}", listenerClassName,
                        jdbcUrl, e);
                }
            }
        }
        log.trace("Attached {} listeners to connection provider for {}", numListeners, jdbcUrl);
    }

    /**
     * Creates an instance of the {@link ConnectionProviderListener} implementation defined in the
     * <strong>listenerClazz</strong> and both constructs an instance, passes in database settings for this connection
     * provider, and injects this ConnectionProviderWrapper into the listener instance
     * @param hibernateProps the hibernate properties, which may be used by the listener to check if it is
     *          enabled/disabled, or to extract configurable values
     * @param listenerClazz the listener implementation class that should be used
     * @return the listener instance that was created or <code>null</code> if there are any exceptions constructing or
     *          initializing the new listener
     */
    private ConnectionProviderListener createListener(Map<Object, Object> hibernateProps,
                                                      Class<? extends ConnectionProviderListener> listenerClazz) {
        // try to locate a constructor that takes ConnectionProviderListenerSettings object first
        Constructor<? extends ConnectionProviderListener> constructor;
        try {
            constructor = listenerClazz.getDeclaredConstructor();

            // make it accessible if it is not already
            if (!constructor.isAccessible()) {
                constructor.setAccessible(true);
            }
        } catch (NoSuchMethodException nsme) {
            log.error("Unable to attach connection provider listener while constructing ConnectionProvider for '{}'. " +
                "{} must declare a zero-argument constructor to be used as a listener", jdbcUrl, listenerClazz);
            return null;
        } catch (SecurityException se) {
            log.error("Unable to make constructor accessible for listener class '{}' when constructing a" +
                " ConnectionProvider for {}", listenerClazz, jdbcUrl, se);
            return null;
        }

        // now that we know it is accessible, instantiate it and attach to this InstrumentedConnectionProvider
        ConnectionProviderListener listener;
        try {
            // instantiate the new listener instance
            listener = constructor.newInstance();
        } catch (IllegalAccessException iae) {
            log.error("Unable to access constructor for listener class '{}' for ConnectionProvider to {}",
                listenerClazz, jdbcUrl, iae);
            return null;
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getTargetException();
            log.error("Unable to create new instance of '{}' because constructor threw in an exception while " +
                "constructing ConnectionProvider for {}", listenerClazz, jdbcUrl, cause);
            return null;
        } catch (InstantiationException ie) {
            Throwable cause = (ie.getCause() != null ? ie.getCause() : ie);
            log.error("Unable to create new instance of '{}' due to an unexpected exception while constructing the" +
                "  ConnectionProvider for {}", listenerClazz, jdbcUrl, cause);
            return null;
        }

        // create the listener settings object, which can be done in a subclass and therefore may throw an exception
        ConnectionProviderListenerSettings settings;
        try {
            // pass in the Settings, which a subclass of this provider can override
            settings = createListenerSettings(hibernateProps);
        } catch (Exception e) {
            log.error("Encountered exception while creating listener settings for {}", jdbcUrl, e);
            return null;
        }

        // finish the initialization and return to caller
        try {
            // inject the parent (this) InstrumentedConnectionProvider into the listener object we just created, but
            //      make sure to do so before we call initialize(..) method
            if (listener instanceof ConnectionProviderAware) {
                ((ConnectionProviderAware) listener).setConnectionProvider(this);
            }

            // allow the listener to initialize itself
            listener.initialize(settings);

            // listener has been fully configured, now return it
            return listener;
        } catch (Exception e) {
            log.error("Unable to initialize listener '{}' when constructing ConnectionProvider for {}", listenerClazz,
                jdbcUrl, e);
            return null;
        }
    }

    /**
     * Adds a given {@link ConnectionProviderListener} to its specific interface-implementing event listener types.
     * This allows us to optimize invocations to listeners based on the event so we are not forced to iterate over all
     * listeners that may not subscribe to that callback.
     *
     * @param listener the listener to add to appropriate listener type lists
     */
    private void addToListenerLists(ConnectionProviderListener listener) {
        Class<? extends ConnectionProviderListener> listenerClass = listener.getClass();
        if (listenerMap.containsKey(listenerClass)) {
            throw new IllegalStateException(String.format("There is already a listener of type '%s' attached to the " +
                "ConnectionProvider for %s", listenerClass, jdbcUrl));
        }

        // add to the connection provider's map of singleton listeners
        listenerMap.put(listenerClass, listener);

        // attach to each phase-specific listener lists, one for each listener interface it implements
        if (listener instanceof PreConnectionAcquisitionListener) {
            preAcquisitionListeners.add((PreConnectionAcquisitionListener)listener);
        }
        if (listener instanceof PostConnectionAcquisitionListener) {
            postAcquisitionListeners.add((PostConnectionAcquisitionListener)listener);
        }
        if (listener instanceof PreConnectionCloseListener) {
            preCloseListeners.add((PreConnectionCloseListener)listener);
        }
        if (listener instanceof PostConnectionCloseListener) {
            postCloseListeners.add((PostConnectionCloseListener)listener);
        }
    }

    /**
     * Removes all listeners from this connection provider.
     * @see #addToListenerLists(ConnectionProviderListener)
     */
    protected void removeAllListeners() {
        preAcquisitionListeners.clear();
        postAcquisitionListeners.clear();
        preCloseListeners.clear();
        postCloseListeners.clear();
    }
}
