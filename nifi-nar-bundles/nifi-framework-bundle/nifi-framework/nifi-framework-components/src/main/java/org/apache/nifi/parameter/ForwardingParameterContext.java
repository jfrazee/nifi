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
package org.apache.nifi.parameter;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;

import java.util.Map;
import java.util.Optional;

// TODO: Override the interface default methods from Authorizable
public class ForwardingParameterContext implements ParameterContext {
    private final ParameterContext c;

    public ForwardingParameterContext(final ParameterContext parameterContext) {
        this.c = parameterContext;
    }

    @Override
    public String getIdentifier() {
        return c.getIdentifier();
    }

    @Override
    public String getName() {
        return c.getName();
    }

    @Override
    public void setName(final String name) {
        c.setName(name);
    }

    @Override
    public String getDescription() {
        return c.getDescription();
    }

    @Override
    public void setDescription(final String description) {
        c.setDescription(description);
    }

    @Override
    public void setParameters(final Map<String, Parameter> updatedParameters) {
        c.setParameters(updatedParameters);
    }

    @Override
    public void verifyCanSetParameters(final Map<String, Parameter> parameters) {
        c.verifyCanSetParameters(parameters);
    }

    @Override
    public Optional<Parameter> getParameter(final String parameterName) {
        return c.getParameter(parameterName);
    }

    @Override
    public Optional<Parameter> getParameter(final ParameterDescriptor parameterDescriptor) {
        return c.getParameter(parameterDescriptor);
    }

    @Override
    public Map<ParameterDescriptor, Parameter> getParameters() {
        return c.getParameters();
    }

    @Override
    public ParameterReferenceManager getParameterReferenceManager() {
        return c.getParameterReferenceManager();
    }

    @Override
    public String getProcessGroupIdentifier() {
        return c.getProcessGroupIdentifier();
    }

    @Override
    public Resource getResource() {
        return c.getResource();
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return c.getParentAuthorizable();
    }

    @Override
    public boolean isEmpty() {
        return c.isEmpty();
    }

    @Override
    public long getVersion() {
        return c.getVersion();
    }
}
