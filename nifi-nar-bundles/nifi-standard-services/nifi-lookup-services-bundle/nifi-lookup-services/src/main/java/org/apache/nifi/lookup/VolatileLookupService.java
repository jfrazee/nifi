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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Tags({"lookup", "cache", "enrich", "join"})
@CapabilityDescription("A local in-memory lookup table backed by a concurrent map")
public class VolatileLookupService extends AbstractControllerService implements MutableLookupService {

    public static final PropertyDescriptor LOOKUP_SERVICE_CAPACITY =
        new PropertyDescriptor.Builder()
            .name("lookup-service-capacity")
            .displayName("Capacity")
            .description("Capacity of the lookup service")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;

    private ConcurrentMap<String, String> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LOOKUP_SERVICE_CAPACITY);
        this.properties = Collections.unmodifiableList(properties);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        Integer capacity =
            context.getProperty(LOOKUP_SERVICE_CAPACITY).asInteger();
        this.cache = new ConcurrentHashMap<>(capacity);
    }

    @OnDisabled
    public void shutdown() {
        cache.clear();
        this.cache = null;
    }

    @Override
    public String get(String id) {
        return cache.get(id);
    }

    @Override
    public String put(String id, String value) {
        return cache.put(id, value);
    }

    @Override
    public String putIfAbsent(String id, String value) {
        return cache.putIfAbsent(id, value);
    }

    @Override
    public Map<String, String> getAll() {
        return Collections.unmodifiableMap(cache);
    }

    @Override
    public void putAll(Map<String, String> values) {
        cache.putAll(values);
    }

}
