package com.fitbit.hibernate.connection.impl;

import static org.mockito.Mockito.mock;

import com.fitbit.hibernate.connection.InstrumentedConnectionProvider;

import com.google.common.base.Ticker;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.connection.ConnectionProvider;
import org.hibernate.connection.ConnectionProviderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionPerformanceMetricListenerUnitTest {

    private static final long ACQUISITION_DELAY_MILLIS = 30L;

    private static final String TEST_JDBC_URL = "jdbc:somedriver://somehost:someport/somedatabase";

    private Properties hibernateProps;
    private TestableInstrumentedConnectionProvider connectionProvider;
    private MockTicker ticker;

    @Mock
    private ConnectionProvider delegatingConnectionProvider;

    private ConnectionMetricReporter connectionMetricReporter;

    @Before
    public void setupConnectionProvider() {
        hibernateProps = new Properties();
        hibernateProps.setProperty(Environment.URL, TEST_JDBC_URL);
        hibernateProps.setProperty(Environment.CONNECTION_PROVIDER,
            TestableInstrumentedConnectionProvider.class.getName());
        hibernateProps.setProperty(InstrumentedConnectionProvider.DELEGATE_CONNECTION_PROVIDER_CLASS,
            MockConnectionProvider.class.getName());
        hibernateProps.setProperty(InstrumentedConnectionProvider.CONNECTION_PROVIDER_LISTENERS,
            ConnectionPerformanceMetricListener.class.getName());
        ticker = new MockTicker();
        connectionMetricReporter = StaticConnectionProviderMetricReporter.getInstance();

        ConnectionProvider connProvider = ConnectionProviderFactory.newConnectionProvider(hibernateProps);
        Assert.assertTrue(connProvider instanceof TestableInstrumentedConnectionProvider);
        connectionProvider = (TestableInstrumentedConnectionProvider) connProvider;
        Assert.assertNotNull(connectionProvider.getWrappedConnectionProvider());
        Assert.assertEquals(TEST_JDBC_URL, connectionProvider.getJdbcUrl());
        ((MockConnectionProvider)connectionProvider.getWrappedConnectionProvider()).setTicker(ticker);

        // make sure it starts with the system ticker
        Assert.assertSame(Ticker.systemTicker(), connectionProvider.getTicker());
        // inject mock ticker
        connectionProvider.setup(ticker, connectionMetricReporter);
        Assert.assertSame(ticker, connectionProvider.getTicker());

        // reset the ticker at the beginning of every test
        ticker.setElapsedMillis(0);

        // reset any recorded metrics from previous tests
        StaticConnectionProviderMetricReporter.getInstance().reset();
    }

    @Test
    public void testMetricsEmptyBeforeRunning() throws Exception {
        StaticConnectionProviderMetricReporter metrics = StaticConnectionProviderMetricReporter.getInstance();
        Assert.assertEquals(0, metrics.getNumAcquisitionFailures());
        Assert.assertEquals(0, metrics.getNumConnectionsAcquired());
        Assert.assertEquals(0, metrics.getNumConnectionsClosed());
        Assert.assertEquals(0L, metrics.getTotalConnectionUsageMillis());
    }

    @Test
    public void testAcquisitionTiming() throws Exception {
        StaticConnectionProviderMetricReporter metrics = StaticConnectionProviderMetricReporter.getInstance();

        getConnection(/*delayMillis=*/null, /*shouldFail=*/false);
        Assert.assertEquals(1, metrics.getNumConnectionsAcquired());
        Assert.assertEquals(0, metrics.getNumConnectionsClosed());
        Assert.assertEquals(0, metrics.getNumAcquisitionFailures());
        // immediately release with no time elapsed
        closeConnection(/*shouldFail=*/false);
        Assert.assertEquals(1, metrics.getNumConnectionsClosed());

        // grab a connection with a delay during acquisition
        getConnection(ACQUISITION_DELAY_MILLIS, /*shouldFail=*/false);
        Assert.assertEquals(2, metrics.getNumConnectionsAcquired());
        Assert.assertEquals(1, metrics.getNumConnectionsClosed());
        Assert.assertEquals(0, metrics.getNumAcquisitionFailures());
        Assert.assertEquals(0L, metrics.getTotalConnectionUsageMillis());
        closeConnection(/*shouldFail=*/false);
        Assert.assertEquals(2, metrics.getNumConnectionsClosed());
    }

    @Test
    public void testConnectionUsageTiming() throws Exception {
        StaticConnectionProviderMetricReporter metrics = StaticConnectionProviderMetricReporter.getInstance();

        getConnection(/*delayMillis=*/null, /*shouldFail=*/false);
        Assert.assertEquals(1, metrics.getNumConnectionsAcquired());
        Assert.assertEquals(0, metrics.getNumConnectionsClosed());
        Assert.assertEquals(0, metrics.getNumAcquisitionFailures());
        Assert.assertEquals(0L, metrics.getTotalConnectionUsageMillis());

        // set elapsed time to 30 ms
        ticker.setElapsedMillis(30);

        closeConnection(/*shouldFail=*/false);
        Assert.assertEquals(1, metrics.getNumConnectionsAcquired());
        Assert.assertEquals(1, metrics.getNumConnectionsClosed());
        Assert.assertEquals(0, metrics.getNumAcquisitionFailures());
        Assert.assertEquals(30L, metrics.getTotalConnectionUsageMillis());
    }

    @Test(expected = SQLException.class)
    public void testCaptureFailures() throws Exception {
        StaticConnectionProviderMetricReporter metrics = StaticConnectionProviderMetricReporter.getInstance();

        Assert.assertEquals(0, metrics.getNumConnectionsAcquired());
        Assert.assertEquals(0, metrics.getNumConnectionsClosed());
        Assert.assertEquals(0, metrics.getNumAcquisitionFailures());
        Assert.assertEquals(0L, metrics.getTotalConnectionUsageMillis());

        try {
            getConnection(/*delayMillis=*/null, /*shouldFail=*/true);
        } finally {
            Assert.assertEquals(0, metrics.getNumConnectionsAcquired());
            Assert.assertEquals(1, metrics.getNumAcquisitionFailures());
        }
    }

    /**
     * Grabs a connection from the provider, introducing an artificial delay if the <strong>delayMillis</strong> arg
     * is provided and the wrapped connection provider is not the mock created using Mockito.
     * @param delayMillis optional delay in milliseconds that the getConnection(..) method should take to execute
     * @param shouldFail if this is <code>true</code> then this method will always throw a {@link SQLException}
     * @see MockConnectionProvider#setAcquisitionDelay(long)
     */
    private Connection getConnection(Long delayMillis, boolean shouldFail) throws SQLException {
        ConnectionProvider wrappedProvider = connectionProvider.getWrappedConnectionProvider();
        if (wrappedProvider instanceof MockConnectionProvider) {
            MockConnectionProvider provider = (MockConnectionProvider) wrappedProvider;
            if (delayMillis != null && delayMillis > 0) {
                provider.setAcquisitionDelay(delayMillis);
            }
            if (shouldFail) {
                provider.setShouldAlwaysFail(true);
            }
        }
        return connectionProvider.getConnection();
    }

    private void closeConnection(boolean shouldFail) throws SQLException {
        ConnectionProvider wrappedProvider = connectionProvider.getWrappedConnectionProvider();
        if (wrappedProvider instanceof MockConnectionProvider && shouldFail) {
            ((MockConnectionProvider)wrappedProvider).setShouldAlwaysFail(shouldFail);
        }
        connectionProvider.closeConnection(null);
    }

    private static class MockTicker extends Ticker {

        private long elapsedNanos;

        /**
         * Sets the number of milliseconds that should have elapsed when this ticker has its {@link #read()} method
         * called.
         */
        public void setElapsedMillis(long elapsedMillis) {
            elapsedNanos = TimeUnit.NANOSECONDS.convert(elapsedMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * Adjusts the elapsed time in <strong>milliseconds</strong>, even though the
         * underlying time value is measured in nanoseconds.
         */
        public void adjustElapsedMillis(long deltaMillis) {
            elapsedNanos += TimeUnit.NANOSECONDS.convert(deltaMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * Returns the number of nanoseconds elapsed since this ticker's fixed
         * point of reference.
         */
        @Override
        public long read() {
            return elapsedNanos;
        }
    }

    public static class MockConnectionProvider implements ConnectionProvider {

        private String jdbcUrl;
        private boolean closed;
        private final ThreadLocal<Long> nextAcquisitionDelayMillis = new ThreadLocal<>();
        private final ThreadLocal<Boolean> shouldAlwaysFail = new ThreadLocal<Boolean>() {
            @Override
            protected Boolean initialValue() {
                return false;
            }
        };
        private MockTicker ticker;

        public void setTicker(MockTicker ticker) {
            this.ticker = ticker;
        }

        @Override
        public void configure(Properties props) throws HibernateException {
            jdbcUrl = props.getProperty(Environment.URL);
        }

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        @Override
        public boolean supportsAggressiveRelease() {
            return true;
        }

        @Override
        public void close() throws HibernateException {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        public void setShouldAlwaysFail(boolean shouldAlwaysFail) {
            this.shouldAlwaysFail.set(shouldAlwaysFail);
        }

        private void throwAndResetIfFailureDesired() throws SQLException {
            Boolean shouldFail = shouldAlwaysFail.get();
            if (shouldFail != null) {
                shouldAlwaysFail.remove();
                if (shouldFail) {
                    throw new SQLException("Intentional ConnectionProvider operation failure");
                }
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            throwAndResetIfFailureDesired();
            // delay returning the acquired connection so we can test the acquisition timings
            Long delayMillis = nextAcquisitionDelayMillis.get();
            if (delayMillis != null) {
                ticker.adjustElapsedMillis(delayMillis);
                nextAcquisitionDelayMillis.remove();
            }
            return mock(Connection.class);
        }

        @Override
        public void closeConnection(Connection conn) throws SQLException {
            throwAndResetIfFailureDesired();
        }

        void setAcquisitionDelay(long acquisitionDelayMillis) {
            nextAcquisitionDelayMillis.set(acquisitionDelayMillis);
        }
    }
}
