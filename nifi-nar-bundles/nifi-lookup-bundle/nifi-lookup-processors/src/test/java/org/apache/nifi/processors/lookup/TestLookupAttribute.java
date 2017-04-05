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
package org.apache.nifi.processors.lookup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.lookup.VolatileLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestLookupAttribute {

    @Test
    public void testLookupAttribute() throws InitializationException {
        final VolatileLookupService service =
            new VolatileLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("volatile-lookup-service", service);
        runner.setProperty(service, VolatileLookupService.LOOKUP_SERVICE_CAPACITY.getName(), "10");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "volatile-lookup-service");

        service.put("key1", "value1");
        service.put("key2", "value2");
        service.put("key3", "value3");

        runner.enqueue("some content".getBytes());
        runner.run(1, false);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);

        Assert.assertNotNull(flowFile);

        flowFile.assertAttributeExists("key1");
        flowFile.assertAttributeExists("key2");
        flowFile.assertAttributeExists("key3");

        flowFile.assertAttributeEquals("key1", "value1");
        flowFile.assertAttributeEquals("key2", "value2");
        flowFile.assertAttributeEquals("key3", "value3");
    }

    @Ignore
    @Test
    public void testLookupAttributeWithDynamicProperties() throws InitializationException {
    }

    @Ignore
    @Test
    public void testLookupAttributeWithPropertiesFileLookupService() throws InitializationException {
    }

}
