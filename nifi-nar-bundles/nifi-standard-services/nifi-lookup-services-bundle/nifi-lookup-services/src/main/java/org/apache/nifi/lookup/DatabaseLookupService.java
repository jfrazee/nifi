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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.DatabaseConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.builder.BasicConfigurationBuilder;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.builder.fluent.DatabaseBuilderParameters;
import org.apache.commons.configuration2.event.EventListener;
import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({"lookup", "cache", "enrich", "join", "jdbc", "database"})
@CapabilityDescription("A reloadable properties file-based lookup service")
public class DatabaseLookupService extends AbstractControllerService implements LookupService {

    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
        .name("connection-pool")
        .displayName("Connection Pool")
        .description("Specifices the JDBC connection pool used to connect to the database.")
        .identifiesControllerService(DBCPService.class)
        .required(true)
        .build();

    static final PropertyDescriptor LOOKUP_TABLE_NAME = new PropertyDescriptor.Builder()
        .name("lookup-table-name")
        .displayName("Lookup Table Name")
        .description("Lookup table name.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    static final PropertyDescriptor LOOKUP_KEY_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-key-column")
            .displayName("Lookup Key Column")
            .description("Lookup key column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-value-column")
            .displayName("Lookup Value Column")
            .description("Lookup value column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor LOOKUP_NAME_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-name-column")
            .displayName("Lookup Name Column")
            .description("Lookup name column.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor LOOKUP_NAME =
        new PropertyDescriptor.Builder()
            .name("lookup-name")
            .displayName("Lookup Name")
            .description("Lookup name.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private List<PropertyDescriptor> properties;

    private BasicConfigurationBuilder<DatabaseConfiguration> builder;

    private Configuration getConfiguration() {
        try {
            if (builder != null) {
                return builder.getConfiguration();
            }
        } catch (final ConfigurationException e) {
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
        super.init(context);
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(CONNECTION_POOL);
        properties.add(LOOKUP_TABLE_NAME);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.add(LOOKUP_VALUE_COLUMN);
        properties.add(LOOKUP_NAME_COLUMN);
        properties.add(LOOKUP_NAME);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, ExecutionException {
        final DBCPService databaseService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
        final DataSource dataSource = databaseService.getDataSource();
        final String lookupTableName = context.getProperty(LOOKUP_TABLE_NAME).getValue();
        final String lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).getValue();
        final String lookupValueColumn = context.getProperty(LOOKUP_VALUE_COLUMN).getValue();
        final String lookupNameColumn = context.getProperty(LOOKUP_NAME_COLUMN).getValue();
        final String lookupName = context.getProperty(LOOKUP_NAME).getValue();

        DatabaseBuilderParameters parameters = new Parameters().database();
        parameters = parameters.setDataSource(dataSource)
            .setTable(lookupTableName)
            .setKeyColumn(lookupKeyColumn)
            .setValueColumn(lookupValueColumn);

        if (StringUtils.isNotBlank(lookupNameColumn)) {
            parameters = parameters.setConfigurationNameColumn(lookupNameColumn).setConfigurationName(lookupName);
        }

        this.builder = new BasicConfigurationBuilder<>(DatabaseConfiguration.class);
        builder.configure(parameters);
    }

    @OnDisabled
    public void onDisabled() {
        this.builder = null;
    }

    public String get(String key) throws IOException {
        final Configuration config = getConfiguration();
        if (config != null) {
            final Object value = config.getProperty(key);
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    public Map<String, String> asMap() throws IOException {
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
