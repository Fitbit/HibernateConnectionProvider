package com.fitbit.hibernate.connection;

import com.fitbit.hibernate.connection.impl.ConnectionMetricReporter;
import com.fitbit.hibernate.connection.impl.ConnectionPerformanceMetricListener;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.SessionFactoryImplementor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

public class InstrumentedConnectionProviderUnitTest {

    private Configuration config;
    private SessionFactory sessionFactory;
    private InstrumentedConnectionProvider connectionProvider;
    private ConnectionPerformanceMetricListener metricListener;

    @Before
    public void setupContext() throws Exception {
        Properties props = new Properties();
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("hibernate.properties")) {
            props.load(is);
        }
        config = new Configuration().addProperties(props);
        sessionFactory = config.buildSessionFactory();
        connectionProvider = (InstrumentedConnectionProvider)((SessionFactoryImplementor)sessionFactory)
            .getSettings().getConnectionProvider();
        metricListener = connectionProvider.getListenerOfType(ConnectionPerformanceMetricListener.class);

        ConnectionMetricReporter.getInstance().reset();
    }

    @Test
    public void testProperSetup() {
        Assert.assertNotNull(config);
        Assert.assertNotNull(sessionFactory);
        Assert.assertNotNull(connectionProvider);
        Assert.assertTrue(sessionFactory.getAllClassMetadata().isEmpty());
        Assert.assertNotNull(metricListener);
    }

    @Test
    public void testIntegratedMetricListener() throws Exception {
        ConnectionMetricReporter stats = ConnectionMetricReporter.getInstance();
        stats.reset();
        Assert.assertEquals(0, stats.getTotalNumOps());
        Assert.assertEquals(0, stats.getNumTopLevelAcquired());
        Connection conn = connectionProvider.getConnection();
        Assert.assertEquals(1, stats.getTotalNumOps());
        Assert.assertEquals(1, stats.getNumTopLevelAcquired());
        Assert.assertEquals(0, stats.getNumAcquisitionFailures());
        Assert.assertEquals(0, stats.getTotalNumClosed());
        connectionProvider.closeConnection(conn);
        Assert.assertEquals(2, stats.getTotalNumOps());
        Assert.assertEquals(1, stats.getNumTopLevelAcquired());
        Assert.assertEquals(1, stats.getNumTopLevelClosed());
    }

    @Test
    public void testAcquisitionDepthLogic() throws Exception {
        ConnectionMetricReporter stats = ConnectionMetricReporter.getInstance();
        Assert.assertEquals(0, stats.getTotalNumOps());
        Assert.assertEquals(0, stats.getNumTopLevelAcquired());
        Connection firstConn = connectionProvider.getConnection();
        Assert.assertEquals(1, stats.getTotalNumOps());
        Assert.assertEquals(1, stats.getNumTopLevelAcquired());
        // starts timing
        Thread.sleep(10L);
        Assert.assertEquals(0, stats.getTotalConnectionUsageMillis());
        // open a second connection before closing
        Connection secondConn = connectionProvider.getConnection();
        // we count this operation but do not count it as an 'acquisition' in our terms due to the fact that we cannot
        //      time how long it was held for
        Assert.assertEquals(1, stats.getNumTopLevelAcquired());
        Assert.assertEquals(2, stats.getTotalNumAcquired());
        Assert.assertEquals(0, stats.getNumTopLevelClosed());
        Assert.assertEquals(0, stats.getTotalConnectionUsageMillis());
        Assert.assertEquals(2, stats.getTotalNumOps());
        connectionProvider.closeConnection(secondConn);
        Assert.assertEquals(0, stats.getNumTopLevelClosed());
        Assert.assertEquals(1, stats.getTotalNumClosed());
        // we should not have timed the nested connection being held
        Assert.assertEquals(0, stats.getTotalConnectionUsageMillis());
        Assert.assertEquals(3, stats.getTotalNumOps());
        connectionProvider.closeConnection(firstConn);
        Assert.assertEquals(1, stats.getNumTopLevelClosed());
        Assert.assertEquals(2, stats.getTotalNumClosed());
        Assert.assertEquals(4, stats.getTotalNumOps());
        // we should still get a timing of >10ms <12ms
        Assert.assertTrue(stats.getTotalConnectionUsageMillis() >= 10);
    }
}
