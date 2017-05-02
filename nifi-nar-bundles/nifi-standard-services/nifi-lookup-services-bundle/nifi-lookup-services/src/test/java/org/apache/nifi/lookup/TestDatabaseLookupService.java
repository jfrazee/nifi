/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.lookup;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class TestDatabaseLookupService {

    final static String DB_LOCATION = "target/db";

    @BeforeClass
    public static void setup() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Test
    public void testDatabaseLookupService() throws InitializationException, IOException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool pool = new DBCPConnectionPool();
        final DatabaseLookupService service = new DatabaseLookupService();

        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        runner.addControllerService("connection-pool", pool);
        runner.setProperty(pool, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(pool, DBCPConnectionPool.DB_USER, "test");
        runner.setProperty(pool, DBCPConnectionPool.DB_PASSWORD, "test");
        runner.setProperty(pool, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.enableControllerService(pool);
        runner.assertValid(pool);

        runner.addControllerService("database-lookup-service", service);
        runner.setProperty(service, DatabaseLookupService.CONNECTION_POOL, "connection-pool");
        runner.setProperty(service, DatabaseLookupService.LOOKUP_TABLE_NAME, "test");
        runner.setProperty(service, DatabaseLookupService.LOOKUP_KEY_COLUMN, "name");
        runner.setProperty(service, DatabaseLookupService.LOOKUP_VALUE_COLUMN, "value");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final DatabaseLookupService lookupService =
            (DatabaseLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("database-lookup-service");

        assertNotNull(lookupService);
        assertThat(lookupService, instanceOf(LookupService.class));

        setup(pool);

        final String property1 = lookupService.get("property.1");
        assertEquals("this is property 1", property1);

        final String property2 = lookupService.get("property.2");
        assertEquals("this is property 2", property2);

        final String property3 = lookupService.get("property.3");
        assertNull(property3);

        update(pool);

        final String updated1 = lookupService.get("property.1");
        assertEquals("this is property 1 updated", updated1);

        final String updated2 = lookupService.get("property.2");
        assertEquals("this is property 2", updated2);

        final String updated3 = lookupService.get("property.3");
        assertEquals("this is property 3", updated3);
    }

    @Test
    public void testDatabaseLookupServiceAsMap() throws InitializationException, IOException, SQLException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final DBCPConnectionPool pool = new DBCPConnectionPool();
        final DatabaseLookupService service = new DatabaseLookupService();

        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        runner.addControllerService("connection-pool", pool);
        runner.setProperty(pool, DBCPConnectionPool.DATABASE_URL, "jdbc:derby:" + DB_LOCATION + ";create=true");
        runner.setProperty(pool, DBCPConnectionPool.DB_USER, "test");
        runner.setProperty(pool, DBCPConnectionPool.DB_PASSWORD, "test");
        runner.setProperty(pool, DBCPConnectionPool.DB_DRIVERNAME, "org.apache.derby.jdbc.EmbeddedDriver");
        runner.enableControllerService(pool);
        runner.assertValid(pool);

        runner.addControllerService("database-lookup-service", service);
        runner.setProperty(service, DatabaseLookupService.CONNECTION_POOL, "connection-pool");
        runner.setProperty(service, DatabaseLookupService.LOOKUP_TABLE_NAME, "test");
        runner.setProperty(service, DatabaseLookupService.LOOKUP_KEY_COLUMN, "name");
        runner.setProperty(service, DatabaseLookupService.LOOKUP_VALUE_COLUMN, "value");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final DatabaseLookupService lookupService =
            (DatabaseLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("database-lookup-service");

        assertNotNull(lookupService);
        assertThat(lookupService, instanceOf(LookupService.class));

        setup(pool);

        final Map<String, String> actual = lookupService.asMap();
        final Map<String, String> expected = new HashMap<>();
        expected.put("property.1", "this is property 1");
        expected.put("property.2", "this is property 2");
        assertEquals(expected, actual);

        update(pool);

        actual.clear();
        actual.putAll(lookupService.asMap());

        expected.clear();
        expected.put("property.1", "this is property 1 updated");
        expected.put("property.2", "this is property 2");
        expected.put("property.3", "this is property 3");
    }

    private void setup(DBCPService pool) throws SQLException {
        final Connection conn = pool.getConnection();
        assertNotNull(conn);

        final Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("DROP TABLE test");
        } catch (final Exception e) {}
        stmt.executeUpdate("CREATE TABLE test (id INT, name VARCHAR(255), value VARCHAR(255))");
        stmt.executeUpdate("INSERT INTO test (id, name, value) VALUES (1, 'property.1', 'this is property 1')");
        stmt.executeUpdate("INSERT INTO test (id, name, value) VALUES (2, 'property.2', 'this is property 2')");

        int n = 0;
        final ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            n++;
        }
        assertEquals(2, n);

        stmt.close();
        conn.close();
    }

    private void update(DBCPService pool) throws SQLException {
        final Connection conn = pool.getConnection();
        assertNotNull(conn);

        final Statement stmt = conn.createStatement();
        stmt.executeUpdate("UPDATE test SET value = 'this is property 1 updated' WHERE id = 1");
        stmt.executeUpdate("INSERT INTO test (id, name, value) VALUES (3, 'property.3', 'this is property 3')");

        int n = 0;
        final ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            n++;
        }
        assertEquals(3, n);

        stmt.close();
        conn.close();
    }

}
