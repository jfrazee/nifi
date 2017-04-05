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
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

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

public class TestPropertiesFileLookupService {

    @Test
    public void testPropertiesFileLookupService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final PropertiesFileLookupService service = new PropertiesFileLookupService();

        runner.addControllerService("properties-file-lookup-service", service);
        runner.setProperty(service, PropertiesFileLookupService.PROPERTIES_FILE, "src/test/resources/test.properties");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final PropertiesFileLookupService lookupService =
            (PropertiesFileLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("properties-file-lookup-service");

        assertThat(lookupService, instanceOf(LookupService.class));

        final String property1 = lookupService.get("property.1");
        assertEquals("this is property 1", property1);

        final String property2 = lookupService.get("property.2");
        assertEquals("this is property 2", property2);

        final String property3 = lookupService.get("property.3");
        assertNull(property3);
    }

    @Test
    public void testPropertiesFileLookupServiceAsMap() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final PropertiesFileLookupService service = new PropertiesFileLookupService();

        runner.addControllerService("properties-file-lookup-service", service);
        runner.setProperty(service, PropertiesFileLookupService.PROPERTIES_FILE, "src/test/resources/test.properties");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final PropertiesFileLookupService lookupService =
            (PropertiesFileLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("properties-file-lookup-service");

        assertThat(lookupService, instanceOf(LookupService.class));

        final Map<String, String> actual = lookupService.asMap();
        final Map<String, String> expected = new HashMap<>();
        expected.put("property.1", "this is property 1");
        expected.put("property.2", "this is property 2");
        assertEquals(expected, actual);
    }

}
