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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

@Tags({"lookup", "cache", "enrich", "join", "properties", "reloadable"})
@CapabilityDescription("A reloadable properties file-based lookup service")
public class CSVFileLookupService extends AbstractControllerService implements LookupService {

    public static final PropertyDescriptor CSV_FILE =
        new PropertyDescriptor.Builder()
            .name("csv-file")
            .displayName("CSV File")
            .description("A CSV file.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOOKUP_KEY_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-key-column")
            .displayName("Lookup Key Column")
            .description("Lookup key column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-value-column")
            .displayName("Lookup Value Column")
            .description("Lookup value column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor IGNORE_DUPLICATES =
        new PropertyDescriptor.Builder()
            .name("ignore-duplicates")
            .displayName("Ignore Duplicates")
            .description("Ignore duplicate keys for records in the CSV file.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private List<PropertyDescriptor> properties;

    private ConcurrentMap<String, String> cache;

    private String csvFile;

    private String lookupKeyColumn;

    private String lookupValueColumn;

    private boolean ignoreDuplicates;

    private SynchronousFileWatcher watcher;

    private final ReentrantLock lock = new ReentrantLock();

    private void loadCache() throws IllegalStateException, IOException {
        if (lock.tryLock()) {
            try {
                final ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug("Loading lookup table from file: " + csvFile);
                }

                final Map<String, String> properties = new HashMap<>();
                final FileReader reader = new FileReader(csvFile);
                final Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader);
                for (final CSVRecord record : records) {
                    final String key = record.get(lookupKeyColumn);
                    final String value = record.get(lookupValueColumn);
                    if (StringUtils.isBlank(key)) {
                        throw new IllegalStateException("Empty lookup key encountered in: " + csvFile);
                    } else if (!ignoreDuplicates && properties.containsKey(key)) {
                        throw new IllegalStateException("Duplicate lookup key encountered: " + key + " in " + csvFile);
                    } else if (ignoreDuplicates && properties.containsKey(key)) {
                        logger.warn("Duplicate lookup key encountered: {} in {}", new Object[]{key, csvFile});
                    }
                    properties.put(key, value);
                }

                this.cache = new ConcurrentHashMap<>(properties);

                if (cache.isEmpty()) {
                    logger.warn("Lookup table is empty after reading file: " + csvFile);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CSV_FILE);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.add(LOOKUP_VALUE_COLUMN);
        properties.add(IGNORE_DUPLICATES);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, FileNotFoundException {
        this.csvFile = context.getProperty(CSV_FILE).getValue();
        this.lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).getValue();
        this.lookupValueColumn = context.getProperty(LOOKUP_VALUE_COLUMN).getValue();
        this.ignoreDuplicates = context.getProperty(IGNORE_DUPLICATES).asBoolean();
        this.watcher = new SynchronousFileWatcher(Paths.get(csvFile), new LastModifiedMonitor(), 60000L);
        try {
            loadCache();
        } catch (final IllegalStateException e) {
            throw new InitializationException(e.getMessage(), e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (cache != null) {
            cache.clear();
            this.cache = null;
        }
        this.watcher = null;
        this.csvFile = null;
        this.lookupKeyColumn = null;
        this.lookupValueColumn = null;
        this.ignoreDuplicates = true;
    }

    @Override
    public String get(String key) throws IOException {
        if (watcher != null && watcher.checkAndReset()) {
            loadCache();
        }
        return cache.get(key);
    }

    @Override
    public Map<String, String> asMap() throws IOException {
        if (watcher != null && watcher.checkAndReset()) {
            loadCache();
        }
        return Collections.unmodifiableMap(cache);
    }

}
