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

import java.lang.reflect.ParameterizedType;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.FileBasedBuilderParameters;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.event.EventListener;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.configuration2.reloading.ReloadingController;

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

/**
 * This abstract class defines a generic {@link LookupService} backed by an
 * Apache Commons Configuration {@link FileBasedConfiguration}.
 *
 */
@Tags({"lookup", "cache", "enrich", "join", "reloadable"})
@CapabilityDescription("A reloadable Apache Commons Configuration file-based lookup service")
public abstract class CommonsConfigurationLookupService<T extends FileBasedConfiguration> extends AbstractControllerService implements LookupService {

    public static final PropertyDescriptor CONFIGURATION_FILE =
        new PropertyDescriptor.Builder()
            .name("configuration-file")
            .displayName("Configuration File")
            .description("A configuration file")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private final Class<T> resultClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];

    private List<PropertyDescriptor> properties;

    private ReloadingFileBasedConfigurationBuilder<T> builder;

    private Configuration getConfiguration() {
        try {
            if (builder != null) {
                return builder.getConfiguration();
            }
        } catch (final ConfigurationException e) {
            // TODO: Need to fail starting the service if this happens
            getLogger().error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONFIGURATION_FILE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String config = context.getProperty(CONFIGURATION_FILE).getValue();
        final FileBasedBuilderParameters params = new Parameters().fileBased().setFile(new File(config));
        this.builder = new ReloadingFileBasedConfigurationBuilder<>(resultClass).configure(params);
        builder.addEventListener(ConfigurationBuilderEvent.CONFIGURATION_REQUEST,
            new EventListener<ConfigurationBuilderEvent>() {
                @Override
                public void onEvent(ConfigurationBuilderEvent event) {
                    if (builder.getReloadingController().checkForReloading(null)) {
                        getLogger().debug("Reloading " + config);
                    }
                }
            });
    }

    @OnDisabled
    public void onDisabled() {
        this.builder = null;
    }

    @Override
    public String get(String key) {
        final Configuration config = getConfiguration();
        if (config != null) {
            final Object value = config.getProperty(key);
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    @Override
    public Map<String, String> asMap() {
        final Configuration config = getConfiguration();
        final Map<String, String> properties = new HashMap<>();
        if (config != null) {
            final Iterator<String> keys = config.getKeys();
            while (keys.hasNext()) {
                final String key = keys.next();
                final Object value = config.getProperty(key);
                properties.put(key, String.valueOf(value));
            }
        }
        return Collections.unmodifiableMap(properties);
    }

}
