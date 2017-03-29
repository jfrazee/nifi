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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Tags({"lookup", "cache", "enrich", "join"})
@CapabilityDescription("A caching lookup service")
public abstract class AbstractCachingLookupService extends AbstractControllerService implements LookupService {

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache size")
            .description("Maximum number of stylesheets to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_EXPIRE_AFTER_WRITE = new PropertyDescriptor.Builder()
            .name("cache-expire-after-write")
            .displayName("Cache TTL after write")
            .description("The cache TTL (time-to-live) or how long to keep keys after they're loaded.")
            .required(true)
            .defaultValue("60 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    protected List<PropertyDescriptor> properties;

    private LoadingCache<String, String> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CACHE_SIZE);
        properties.add(CACHE_EXPIRE_AFTER_WRITE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheExpire = context.getProperty(CACHE_EXPIRE_AFTER_WRITE).asTimePeriod(TimeUnit.SECONDS);
        if (cacheSize > 0) {
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheExpire > 0) {
                cacheBuilder = cacheBuilder.expireAfterWrite(cacheExpire, TimeUnit.SECONDS);
            }

            this.cache = cacheBuilder.build(
               new CacheLoader<String, String>() {
                   public String load(String key) {
                       return load(key);
                   }
               });
        } else {
            this.cache = null;
            getLogger().warn("Lookup service cache disabled because cache size is set to 0");
        }
    }

    @OnDisabled
    public void shutdown() {
        if (cache != null) {
            cache.invalidateAll();
            this.cache = null;
        }
    }

    protected abstract String load(String key);

    protected abstract Map<String, String> loadAll();

    @Override
    public String get(String key) {
        if (cache != null) {
            return cache.getIfPresent(key);
        } else {
            return load(key);
        }
    }

    @Override
    public Map<String, String> asMap() {
        final Map<String, String> map;
        if (cache != null) {
            map = cache.asMap();
        } else {
            map = loadAll();
        }
        return Collections.unmodifiableMap(map);
    }

}
