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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({"lookup", "cache", "enrich", "join", "volatile"})
@CapabilityDescription("An volatile, in-memory lookup service")
public class VolatileLookupService extends AbstractControllerService implements MutableLookupService {

    public static final PropertyDescriptor CAPACITY =
        new PropertyDescriptor.Builder()
            .name("capacity")
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
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CAPACITY);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final Integer capacity = context.getProperty(CAPACITY).asInteger();
        this.cache = new ConcurrentHashMap<>(capacity);
    }

    @OnDisabled
    public void shutdown() {
        if (cache != null) {
            cache.clear();
            this.cache = null;            
        }
    }

    @Override
    public String get(String key) {
        return cache.get(key);
    }

    @Override
    public String put(String key, String value) {
        return cache.put(key, value);
    }

    @Override
    public String putIfAbsent(String key, String value) {
        return cache.putIfAbsent(key, value);
    }

    @Override
    public void putAll(Map<String, String> values) {
        cache.putAll(values);
    }

    @Override
    public Map<String, String> asMap() {
        return Collections.unmodifiableMap(cache);
    }

}
