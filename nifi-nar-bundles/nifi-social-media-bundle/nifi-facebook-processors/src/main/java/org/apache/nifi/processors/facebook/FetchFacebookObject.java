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
package org.apache.nifi.processors.facebook;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

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

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.json.JsonObject;
import com.restfb.types.FacebookType;
import com.restfb.types.NamedFacebookType;

import okhttp3.HttpUrl;

import org.apache.nifi.facebook.FacebookRateLimitException;
import org.apache.nifi.facebook.FacebookService;
import org.apache.nifi.facebook.StandardFacebookService;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Facebook", "social"})
@CapabilityDescription("Fetch a Facebook object")
@WritesAttributes({
    @WritesAttribute(attribute="facebook.object.path", description="The object path fetched"),
    @WritesAttribute(attribute="facebook.paging.previous", description="The path to the previous object for paginated resources"),
    @WritesAttribute(attribute="facebook.paging.next", description="The path to the next object for paginated resources"),
})
public class FetchFacebookObject extends AbstractProcessor {

    public static final PropertyDescriptor FB_OBJECT_FIELDS = new PropertyDescriptor
            .Builder().name("facebook-object-fields")
            .displayName("Fields")
            .description("Facebook object fields to fetch")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FB_API_SERVICE =
        new PropertyDescriptor.Builder()
            .name("facebook-api-service")
            .displayName("Facebook Service")
            .description("The Facebook API service to use")
            .identifiesControllerService(FacebookService.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("failure")
            .build();

    public static final Relationship REL_PREVIOUS = new Relationship.Builder()
            .name("previous")
            .description("previous")
            .build();

    public static final Relationship REL_NEXT = new Relationship.Builder()
            .name("next")
            .description("next")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(FB_OBJECT_FIELDS);
        descriptors.add(FB_API_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_PREVIOUS);
        relationships.add(REL_NEXT);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String fbObjectFields = context.getProperty(FB_OBJECT_FIELDS)
            .evaluateAttributeExpressions(flowFile)
            .getValue();

        final FacebookService fbService = context.getProperty(FB_API_SERVICE)
            .asControllerService(FacebookService.class);

        if (fbService.isRateLimited()) {
            getLogger().warn("Rate limited for {} minutes, penalizing FlowFile {} and yielding context", new Object[]{getMinutesFromNow(fbService.getResetTime()), flowFile});
            session.penalize(flowFile);
            context.yield();
            return;
        }

        final AtomicReference<FacebookRateLimitException> fbRateLimitExceptionRef = new AtomicReference<>();
        final AtomicReference<String> fbObjectPathRef = new AtomicReference<>();
        final AtomicReference<JsonObject> fbObjectRef = new AtomicReference<>();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                try {
                    final String fbObjectPath = StringUtils.trimToEmpty(IOUtils.toString(in, StandardCharsets.UTF_8));
                    if (StringUtils.isNotEmpty(fbObjectPath)) {
                        fbObjectPathRef.set(fbObjectPath);
                    } else {
                        getLogger().error("Object path was empty for FlowFile {}", new Object[]{flowFile});
                        return;
                    }

                    final JsonObject fbObject = fbService.fetchObject(fbObjectPath, fbObjectFields);
                    if (fbObject != null && fbObject.length() > 0) {
                        fbObjectRef.set(fbObject);
                    } else {
                        getLogger().warn("Object at {} was empty for FlowFile {}", new Object[]{fbObjectPath, flowFile});
                    }
                } catch (final FacebookRateLimitException e) {
                    fbRateLimitExceptionRef.set(e);
                } catch (final Exception e) {
                    getLogger().error(e.getMessage(), e);
                }
            }
        });

        final String fbObjectPath = fbObjectPathRef.get();
        final FacebookRateLimitException fbRateLimitException = fbRateLimitExceptionRef.get();

        if (fbRateLimitException != null) {
            getLogger().warn("Rate limited while fetching object at {}, penalizing FlowFile {} and yielding context for {} minutes", new Object[]{Objects.toString(fbObjectPath), flowFile, getMinutesFromNow(fbService.getResetTime())});
            session.penalize(flowFile);
            context.yield();
            return;
        }

        final JsonObject fbObject = fbObjectRef.get();
        if (fbObject == null || fbObject.length() <= 0) {
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("facebook.object.path", fbObjectPath);
        attributes.put("mime.type", "application/json");
        attributes.put("mime.extension", ".json");

        final String fbObjectPrevious = getPreviousObjectPath(fbObject);
        if (StringUtils.isNotEmpty(fbObjectPrevious)) {
            FlowFile prevFlowFile = session.clone(flowFile);
            prevFlowFile = session.write(prevFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(fbObjectPrevious.getBytes(StandardCharsets.UTF_8));
                }
            });
            session.transfer(prevFlowFile, REL_PREVIOUS);
            attributes.put("facebook.paging.previous", fbObjectPrevious);
        }

        final String fbObjectNext = getNextObjectPath(fbObject);
        if (StringUtils.isNotEmpty(fbObjectNext)) {
            FlowFile nextFlowFile = session.clone(flowFile);
            nextFlowFile = session.write(nextFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(fbObjectNext.getBytes(StandardCharsets.UTF_8));
                }
            });
            session.transfer(nextFlowFile, REL_NEXT);
            attributes.put("facebook.paging.next", fbObjectNext);
        }

        FlowFile fbObjectFlowFile = session.clone(flowFile);
        fbObjectFlowFile = session.write(fbObjectFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(Objects.toString(fbObject).getBytes(StandardCharsets.UTF_8));
            }
        });
        fbObjectFlowFile = session.putAllAttributes(fbObjectFlowFile, attributes);

        session.transfer(fbObjectFlowFile, REL_SUCCESS);
        session.remove(flowFile);
    }

    private String getPageObjectPath(final JsonObject fbObject, final String pageKey) {
        try {
            final JsonObject fbObjectPaging = fbObject.optJsonObject("paging");
            if (fbObjectPaging != null) {
                final String fbObjectPage = fbObjectPaging.optString(pageKey);
                if (StringUtils.isNotEmpty(fbObjectPage)) {
                    final HttpUrl fbObjectPageURL = HttpUrl.parse(fbObjectPage);
                    final List<String> fbObjectPageParams = new ArrayList<>();
                    for (int i = 0, size = fbObjectPageURL.querySize(); i < size; i++) {
                        final String name = fbObjectPageURL.queryParameterName(i);
                        final String value = fbObjectPageURL.queryParameterValue(i);
                        if (!name.equals("access_token")) {
                            fbObjectPageParams.add(name + "=" + value);
                        }
                    }
                    final String fbPageObjectPath = fbObjectPageURL.encodedPath().replaceAll("^(\\/v[0-9]+(\\.[0-9]+))?(\\/?.*)$", "$3");
                    final String fbPageObjectQuery = String.join("&", fbObjectPageParams);
                    return fbPageObjectPath + "?" + fbPageObjectQuery;
                }
            }
        } catch (final Exception e) {
            getLogger().warn(e.getMessage(), e);
        }
        return null;
    }

    private String getPreviousObjectPath(final JsonObject fbObject) {
        return getPageObjectPath(fbObject, "previous");
    }

    private String getNextObjectPath(final JsonObject fbObject) {
        return getPageObjectPath(fbObject, "next");
    }

    private long getMinutesFromNow(long millis) {
        final long delta = millis - System.currentTimeMillis();
        if (delta > 0) {
            return TimeUnit.MILLISECONDS.toMinutes(delta);
        } else {
            return 0;
        }
    }

}
