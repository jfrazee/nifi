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
package org.apache.nifi.processors.standard;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.SimpleKeyValueLookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestLookupAttribute {

    @Test
    public void testKeyValueLookupAttribute() throws InitializationException {
        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.setProperty(service, "key2", "value2");
        runner.setProperty(service, "key3", "value3");
        runner.setProperty(service, "key4", "  ");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "simple-key-value-lookup-service");
        runner.setProperty(LookupAttribute.LOOKUP_STRATEGY, LookupAttribute.KEY_VALUE_STRATEGY);
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "true");
        runner.setProperty("foo", "key1");
        runner.setProperty("bar", "key2");
        runner.setProperty("baz", "${attr1}");
        runner.setProperty("qux", "key4");
        runner.setProperty("zab", "key5");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr1", "key3");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);

        assertNotNull(flowFile);

        flowFile.assertAttributeExists("foo");
        flowFile.assertAttributeExists("bar");
        flowFile.assertAttributeExists("baz");
        flowFile.assertAttributeExists("qux");
        flowFile.assertAttributeExists("zab");
        flowFile.assertAttributeNotExists("zar");

        flowFile.assertAttributeEquals("foo", "value1");
        flowFile.assertAttributeEquals("bar", "value2");
        flowFile.assertAttributeEquals("baz", "value3");
        flowFile.assertAttributeEquals("qux", "");
        flowFile.assertAttributeEquals("zab", "null");
    }

    @Test
    public void testLookupAttributeUnmatched() throws InitializationException {
        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.setProperty(service, "key2", "value2");
        runner.setProperty(service, "key3", "value3");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "simple-key-value-lookup-service");
        runner.setProperty(LookupAttribute.LOOKUP_STRATEGY, LookupAttribute.KEY_VALUE_STRATEGY);
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "false");
        runner.setProperty("baz", "${attr1}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr1", "key4");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_UNMATCHED, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_UNMATCHED).get(0);

        assertNotNull(flowFile);

        flowFile.assertAttributeExists("attr1");
        flowFile.assertAttributeNotExists("baz");
        flowFile.assertAttributeEquals("attr1", "key4");
    }

    @Test
    public void testMultiStrategyLookupAttribute() throws InitializationException {
        final CoordinateLookupService service = new CoordinateLookupService("x", "y");
        service.addValue("1", "2", "value12");

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("coordinate-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "coordinate-lookup-service");
        runner.setProperty(LookupAttribute.LOOKUP_STRATEGY, LookupAttribute.MULTI_STRATEGY);
        runner.setProperty(LookupAttribute.DESTINATION_ATTRIBUTE, "lookup.result");
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "true");
        runner.setProperty("x", "1");
        runner.setProperty("y", "${attr1}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr1", "2");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_SUCCESS).get(0);

        assertNotNull(flowFile);

        flowFile.assertAttributeExists("lookup.result");
        flowFile.assertAttributeEquals("lookup.result", "value12");
    }

    @Test
    public void testMultiLookupAttributeUnmatched() throws InitializationException {
        final CoordinateLookupService service = new CoordinateLookupService("x", "y");
        service.addValue("1", "2", "value12");

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("coordinate-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "coordinate-lookup-service");
        runner.setProperty(LookupAttribute.LOOKUP_STRATEGY, LookupAttribute.MULTI_STRATEGY);
        runner.setProperty(LookupAttribute.DESTINATION_ATTRIBUTE, "lookup.result");
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "false");
        runner.setProperty("x", "3");
        runner.setProperty("y", "4");
        runner.assertValid();

        runner.enqueue("some content".getBytes());
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_UNMATCHED, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_UNMATCHED).get(0);

        assertNotNull(flowFile);

        flowFile.assertAttributeNotExists("lookup.result");
    }

    @Test
    public void testCustomValidateInvalidMultiStrategy() throws InitializationException {
        final CoordinateLookupService service = new CoordinateLookupService("x", "y");
        service.addValue("1", "2", "value12");

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("coordinate-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "coordinate-lookup-service");
        runner.setProperty(LookupAttribute.LOOKUP_STRATEGY, LookupAttribute.MULTI_STRATEGY);
        runner.assertNotValid();
    }

    private static class CoordinateLookupService extends AbstractControllerService implements StringLookupService {

        private final Map<String, String> values = new HashMap<>();

        private String xName;

        private String yName;

        public CoordinateLookupService(String xName, String yName) {
            if (StringUtils.isBlank(xName) || StringUtils.isBlank(yName)) {
                throw new IllegalArgumentException();
            }
            this.xName = xName;
            this.yName = yName;
        }

        public void addValue(final String x, final String y, final String value) {
            if (StringUtils.isBlank(x) || StringUtils.isBlank(y)) {
                throw new IllegalArgumentException();
            }
            values.put(xName + "=" + x + "&" + yName + "=" + y, value);
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @Override
        public Optional<String> lookup(final Map<String, String> coordinates) {
            if (coordinates == null) {
                return Optional.empty();
            }

            final String x = coordinates.get(xName);
            final String y = coordinates.get(yName);
            if (x == null || y == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(values.get(xName + "=" + x + "&" + yName + "=" + y));
        }

        @Override
        public Set<String> getRequiredKeys() {
            final Set<String> requiredKeys = new HashSet<>();
            requiredKeys.add(xName);
            requiredKeys.add(yName);
            return Collections.unmodifiableSet(requiredKeys);
        }

    }

}
