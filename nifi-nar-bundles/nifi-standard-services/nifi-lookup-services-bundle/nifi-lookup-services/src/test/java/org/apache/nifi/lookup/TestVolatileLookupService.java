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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class TestVolatileLookupService {

    @Test
    public void testVolatileLookupService() throws InitializationException {
        final VolatileLookupService service = new VolatileLookupService();

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("volatile-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertThat(service, instanceOf(MutableLookupService.class));

        final String put1 = service.put("key1", "value1");
        assertNull(put1);

        final String get1 = service.get("key1");
        assertEquals("value1", get1);

        final String put2 = service.put("key1", "value2");
        assertEquals("value1", put2);

        final String get2 = service.get("key1");
        assertEquals("value2", get2);

        final String get3 = service.get("key3");
        assertNull(get3);
    }

    @Test
    public void testVolatileLookupServiceAsMap() throws InitializationException {
        final VolatileLookupService service = new VolatileLookupService();

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("volatile-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        service.put("key1", "value1");
        service.put("key2", "value2");
        service.put("key3", "value3");

        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");

        final Map<String, String> actual = service.asMap();
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    @Test
    public void testVolatileLookupServicePutAll() throws InitializationException {
        final VolatileLookupService service = new VolatileLookupService();

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("volatile-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");

        service.putAll(expected);

        final Map<String, String> actual = service.asMap();
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
    }

    @Test
    public void testVolatileLookupServicePutIfAbsent() throws InitializationException {
        final VolatileLookupService service = new
            VolatileLookupService();

        final TestRunner runner =
            TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("volatile-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final String put1 = service.putIfAbsent("key1", "value1");
        assertNull(put1);

        final String get1 = service.get("key1");
        assertEquals("value1", get1);

        final String put2 = service.putIfAbsent("key1", "value2");
        assertNotNull(put2);
        assertEquals("value1", put2);

        final String get2 = service.get("key1");
        assertEquals("value1", get2);
    }

    @Test
    public void testVolatileLookupServiceWithDynamicProperties() throws InitializationException {
        final VolatileLookupService service = new VolatileLookupService();

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("volatile-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertThat(service, instanceOf(MutableLookupService.class));

        final String get1 = service.get("key1");
        assertEquals("value1", get1);

        final String put2 = service.put("key1", "value2");
        assertEquals("value1", put2);

        final String get2 = service.get("key1");
        assertEquals("value2", get2);

        final String get3 = service.get("key3");
        assertNull(get3);
    }

    @Test
    public void testVolatileLookupServiceWithDynamicPropertiesAsMap() throws InitializationException {
        final VolatileLookupService service = new VolatileLookupService();

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("volatile-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.setProperty(service, "key2", "value2");
        runner.setProperty(service, "key3", "value3");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Map<String, String> expected = new HashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");

        final Map<String, String> actual = service.asMap();
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
    }

}
