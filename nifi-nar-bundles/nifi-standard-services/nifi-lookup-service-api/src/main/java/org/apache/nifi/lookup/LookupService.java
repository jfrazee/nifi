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

import java.io.IOException;
import java.util.Map;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * This interface defines an API that can be used for interacting with a
 * a lookup table. LookupService is used by {@link LookupAttribute} to enrich
 * FlowFiles with attributes from a reference dataset, database or cache.
 *
 */
@Tags({"lookup", "cache", "enrich", "join"})
@CapabilityDescription("Lookup service")
public interface LookupService extends ControllerService {

    /**
     * Returns the value in the lookup table for the given key, if one exists;
     * otherwise returns <code>null</code>
     *
     * @param key the key to lookup
     *
     * @return the value in the backing service for the given key, if
     * one exists; otherwise returns <code>null</code>
     * @throws IOException if the backing service is unavailable or the table
     * cannot be loaded
     */
    String get(String key) throws IOException;

    /**
     * Returns the entire lookup table as a {@link Map}
     *
     * @return the lookup table stored in the backing service
     * @throws IOException if the backing service is unavailable or the table
     * cannot be loaded
     */
    Map<String, String> asMap() throws IOException;

}
