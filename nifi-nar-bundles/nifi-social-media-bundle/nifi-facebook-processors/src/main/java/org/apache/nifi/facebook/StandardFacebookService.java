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
package org.apache.nifi.facebook;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.logging.ComponentLog;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.json.JsonObject;
import com.restfb.types.FacebookType;
import com.restfb.types.NamedFacebookType;
import com.restfb.exception.FacebookGraphException;

import com.google.common.util.concurrent.RateLimiter;

@Tags({"Facebook", "social"})
@CapabilityDescription("Standard implementation of Facebook API service.")
public class StandardFacebookService extends AbstractControllerService implements FacebookService {

    public static final PropertyDescriptor FB_APP_ID = new PropertyDescriptor
            .Builder().name("facebook-app-id")
            .displayName("Application ID")
            .description("Facebook application ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FB_APP_SECRET = new PropertyDescriptor
            .Builder().name("facebook-app-secret")
            .displayName("Application Secret")
            .description("Facebook application secret")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FB_ACCESS_TOKEN = new PropertyDescriptor
            .Builder().name("facebook-access-token")
            .displayName("Access Token")
            .description("Facebook access token")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FB_API_VERSION = new PropertyDescriptor.Builder()
            .name("facebook-api-version")
            .displayName("API Version")
            .description("Facebook API version")
            .required(true)
            .allowableValues(
                Version.VERSION_2_8.getUrlElement(),
                Version.VERSION_2_7.getUrlElement(),
                Version.VERSION_2_6.getUrlElement(),
                Version.VERSION_2_5.getUrlElement(),
                Version.VERSION_2_4.getUrlElement(),
                Version.VERSION_2_3.getUrlElement(),
                Version.VERSION_2_2.getUrlElement(),
                Version.VERSION_2_1.getUrlElement()
            )
            .defaultValue(Version.LATEST.getUrlElement())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> descriptors;

    static {
        final List<PropertyDescriptor> _descriptors = new ArrayList<>();
        _descriptors.add(FB_APP_ID);
        _descriptors.add(FB_APP_SECRET);
        _descriptors.add(FB_ACCESS_TOKEN);
        _descriptors.add(FB_API_VERSION);
        descriptors = Collections.unmodifiableList(_descriptors);
    }

    private final AtomicReference<DefaultFacebookClient> fbClientRef = new AtomicReference<>();

    private final AtomicLong resetTimeRef = new AtomicLong(0);

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String appId = context.getProperty(FB_APP_ID).getValue();
        final String appSecret = context.getProperty(FB_APP_SECRET).getValue();
        final String accessToken = context.getProperty(FB_ACCESS_TOKEN).getValue();
        final Version apiVersion = Version.getVersionFromString(context.getProperty(FB_API_VERSION).getValue());
        final DefaultFacebookClient fbClient = new DefaultFacebookClient(accessToken, appSecret, apiVersion);
        fbClientRef.set(fbClient);
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        fbClientRef.set(null);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    public JsonObject fetchObject(String fbObjectPath, String fbObjectFields) throws FacebookRateLimitException {
        final DefaultFacebookClient fbClient = fbClientRef.get();
        if (fbClient != null) {
            try {
                final URI fbObjectURI = new URI(fbObjectPath);
                final String fbObjectId = fbObjectURI.getPath();
                final String fbObjectQuery = fbObjectURI.getQuery();
                final List<Parameter> fbObjectParams = new ArrayList<>();

                // if (includeMetadata) {
                //     fbObjectParams.add(Parameter.with("metadata", 1));
                // }

                return fbClient.fetchObject(fbObjectId, JsonObject.class, fbObjectParams.toArray(new Parameter[]{}));
            } catch (final FacebookGraphException e) {
                handleGraphException(e, fbObjectPath);
            } catch (final URISyntaxException e) {
                getLogger().error(e.getMessage(), e);
            }
        }
        return null;
    }

    public boolean isRateLimited() {
        final Long resetTime = getResetTime();
        return (resetTime != null && resetTime > System.currentTimeMillis());
    }

    public Long getResetTime() {
        final long resetTime = resetTimeRef.get();
        if (resetTime > System.currentTimeMillis()) {
            return resetTime;
        } else if (resetTime > 0) {
            getLogger().warn("Reset time is in the past, setting to 0");
            resetTimeRef.set(0);
        }
        return null;
    }

    private void setResetTime(long resetTime) {
        if (resetTime > System.currentTimeMillis()) {
            getLogger().info("Setting reset time to {}", new Object[]{resetTime / 1000});
            resetTimeRef.set(resetTime);
        } else if (resetTime > 0) {
            getLogger().warn("Reset time is in the past, setting to 0");
            resetTimeRef.set(0);
        }
    }

    private void handleGraphException(final FacebookGraphException e, final String fbObjectPath) {
        final Integer errorCode = e.getErrorCode();
        if (errorCode != null && (errorCode == 4 || errorCode == 32)) {
            final ComponentLog logger = getLogger();

            logger.warn("Rate limited while fetching object at {}", new Object[]{fbObjectPath});
            if (logger.isDebugEnabled()) {
                logger.debug(Objects.toString(e.getRawErrorJson()));
            }

            final long resetTime = System.currentTimeMillis() + (60 * 60 * 1000);
            setResetTime(resetTime);

            throw new FacebookRateLimitException(e, resetTime);
        } else {
            throw e;
        }
    }

}
