package com.github.gquintana.metrics.sql;

/*
 * #%L
 * Metrics SQL
 * %%
 * Copyright (C) 2014 Open-Source
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.*;

import static org.junit.Assert.*;

/**
 * Test Statement wrapper
 */
public class ResultSetTest {
    private DropwizardMeterRegistry meterRegistry;
    private JdbcProxyFactory proxyFactory;
    private DataSource rawDataSource;
    private DataSource dataSource;
    @Before
    public void setUp() throws SQLException {
        meterRegistry = MeterRegistryHelper.createDropwizardMeterRegistry();
        proxyFactory = new JdbcProxyFactory(meterRegistry);
        rawDataSource = H2DbUtil.createDataSource();
        try(Connection connection = rawDataSource.getConnection()) {
            H2DbUtil.initTable(connection);
        }
        dataSource = proxyFactory.wrapDataSource(rawDataSource);
    }
    @After
    public void tearDown() throws SQLException {
        try(Connection connection = rawDataSource.getConnection()) {
            H2DbUtil.dropTable(connection);
        }
        H2DbUtil.close(dataSource);
    }
    @Test
    public void testResultSetLife() throws SQLException {
        // Act
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from METRICS_TEST");
        while(resultSet.next()) {
            int id = resultSet.getInt("ID");
            String text = resultSet.getString("TEXT");
            Timestamp timestamp = resultSet.getTimestamp("CREATED");
        }
        H2DbUtil.close(resultSet, statement, connection);
        // Assert
        assertNotNull(connection);
        assertTrue(Proxy.isProxyClass(resultSet.getClass()));
        Timer timer = meterRegistry.getDropwizardRegistry().getTimers().get("javaSqlResultSet[select * from metrics_test]");
        assertNotNull(timer);
        assertEquals(1L, timer.getCount());
        Meter meter = meterRegistry.getDropwizardRegistry().meter("javaSqlResultSet[select * from metrics_test]Rows");
        assertNotNull(meter);
        assertEquals(11L, meter.getCount());
    }
    @Test
    public void testResultSetUnwrap() throws SQLException {
        // Act
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from METRICS_TEST");
        
        // Assert
        assertTrue(resultSet.isWrapperFor(org.h2.jdbc.JdbcResultSet.class));
        assertTrue(resultSet.unwrap(org.h2.jdbc.JdbcResultSet.class) instanceof org.h2.jdbc.JdbcResultSet);
        
        H2DbUtil.close(resultSet, statement, connection);
    }


    @Test
    public void testResultSet_Direct() throws SQLException {
        // Act
        Connection connection = rawDataSource.getConnection();
        Statement statement = connection.createStatement();
        String sql = "select * from METRICS_TEST order by ID";
        ResultSet resultSet = MetricsSql.forRegistry(meterRegistry).wrap(statement.executeQuery(sql), sql);

        H2DbUtil.close(resultSet, statement, connection);
        // Assert
        assertNotNull(connection);
        assertTrue(Proxy.isProxyClass(resultSet.getClass()));
        assertEquals(1, meterRegistry.getDropwizardRegistry().getTimers().get("javaSqlResultSet[select * from metrics_test order by id]").getCount());
    }

}
