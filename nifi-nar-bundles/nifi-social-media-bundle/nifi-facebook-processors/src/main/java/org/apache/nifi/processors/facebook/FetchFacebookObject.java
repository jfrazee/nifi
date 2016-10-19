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
import java.util.concurrent.atomic.AtomicReference;

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

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Facebook", "social"})
@CapabilityDescription("Fetch a Facebook object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FetchFacebookObject extends AbstractProcessor {

    public static final PropertyDescriptor FB_OBJECT_FIELDS = new PropertyDescriptor
            .Builder().name("facebook-object-fields")
            .displayName("Fields")
            .description("Facebook object fields to fetch")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

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

    private FacebookClient fbClient;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(FB_OBJECT_FIELDS);
        descriptors.add(FB_APP_ID);
        descriptors.add(FB_APP_SECRET);
        descriptors.add(FB_ACCESS_TOKEN);
        descriptors.add(FB_API_VERSION);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String appId = context.getProperty(FB_APP_ID).getValue();
        final String appSecret = context.getProperty(FB_APP_SECRET).getValue();
        final String accessToken = context.getProperty(FB_ACCESS_TOKEN).getValue();
        final Version apiVersion = Version.getVersionFromString(context.getProperty(FB_API_VERSION).getValue());
        fbClient = new DefaultFacebookClient(accessToken, appSecret, apiVersion);
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

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("mime.type", "application/json");
        attributes.put("mime.extension", ".json");

        String pageDepth = flowFile.getAttribute("facebook.paging.depth");
        if (StringUtils.isEmpty(pageDepth)) {
            pageDepth = "0";
        }

        try {
            pageDepth = String.valueOf(Integer.parseInt(pageDepth) + 1);
        } catch (final Exception e) {
            getLogger().warn(e.getMessage(), e);
            getLogger().warn("Setting page depth to 1");
            pageDepth = "1";
        }
        attributes.put("facebook.paging.depth", pageDepth);

        final AtomicReference<String> fbObjectRef = new AtomicReference<>();
        final AtomicReference<String> prevRef = new AtomicReference<>();
        final AtomicReference<String> nextRef = new AtomicReference<>();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                try {
                    final String fbObjectPath = StringUtils.trimToEmpty(IOUtils.toString(in, StandardCharsets.UTF_8));
                    if (StringUtils.isEmpty(fbObjectPath)) {
                        getLogger().error("Object path was empty for FlowFile {}", new Object[]{flowFile});
                        return;
                    }

                    final JsonObject fbObject = fetchObject(fbObjectPath, fbObjectFields);
                    if (fbObject != null && fbObject.length() > 0) {
                        attributes.put("facebook.object.path", fbObjectPath);

                        final String fbObjectPrevious = getPreviousObjectPath(fbObject);
                        if (StringUtils.isNotEmpty(fbObjectPrevious)) {
                            prevRef.set(fbObjectPrevious);
                        }

                        final String fbObjectNext = getNextObjectPath(fbObject);
                        if (StringUtils.isNotEmpty(fbObjectNext)) {
                            nextRef.set(fbObjectNext);
                        }

                        fbObjectRef.set(Objects.toString(fbObject));
                    } else {
                        getLogger().warn("Object at {} was empty for FlowFile {}", new Object[]{fbObjectPath, flowFile});
                    }
                } catch (final Exception e) {
                    getLogger().error(e.getMessage(), e);
                }
            }
        });

        final String fbObject = fbObjectRef.get();
        if (StringUtils.isEmpty(fbObject)) {
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        final String prev = prevRef.get();
        if (StringUtils.isNotEmpty(prev)) {
            FlowFile prevFlowFile = session.clone(flowFile);
            prevFlowFile = session.write(prevFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(prev.getBytes(StandardCharsets.UTF_8));
                }
            });
            prevFlowFile = session.putAttribute(prevFlowFile, "facebook.paging.depth", pageDepth);
            session.transfer(prevFlowFile, REL_PREVIOUS);
            attributes.put("facebook.paging.previous", prev);
        }

        final String next = nextRef.get();
        if (StringUtils.isNotEmpty(next)) {
            FlowFile nextFlowFile = session.clone(flowFile);
            nextFlowFile = session.write(nextFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(next.getBytes(StandardCharsets.UTF_8));
                }
            });
            nextFlowFile = session.putAttribute(nextFlowFile, "facebook.paging.depth", pageDepth);
            session.transfer(nextFlowFile, REL_NEXT);
            attributes.put("facebook.paging.next", next);
        }

        FlowFile fbObjectFlowFile = session.clone(flowFile);
        fbObjectFlowFile = session.write(fbObjectFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(fbObject.getBytes(StandardCharsets.UTF_8));
            }
        });
        fbObjectFlowFile = session.putAllAttributes(fbObjectFlowFile, attributes);

        session.transfer(fbObjectFlowFile, REL_SUCCESS);
        session.remove(flowFile);
    }

    private JsonObject fetchObject(final String fbObjectPath, final String fbObjectFields) throws URISyntaxException {
        final URI fbObjectURI = new URI(fbObjectPath);
        final String fbObjectId = fbObjectURI.getPath();
        final String fbObjectQuery = fbObjectURI.getQuery();
        final List<Parameter> fbObjectParams = new ArrayList<>();
        // if (includeMetadata) {
        //     fbObjectParams.add(Parameter.with("metadata", 1));
        // }

        final JsonObject fbObject = fbClient.fetchObject(fbObjectId, JsonObject.class, fbObjectParams.toArray(new Parameter[]{}));

        return fbObject;
    }

    private String getPageObjectPath(final JsonObject fbObject, final String pageKey) throws MalformedURLException {
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
        return null;
    }

    private String getPreviousObjectPath(final JsonObject fbObject) throws MalformedURLException {
        return getPageObjectPath(fbObject, "previous");
    }

    private String getNextObjectPath(final JsonObject fbObject) throws MalformedURLException {
        return getPageObjectPath(fbObject, "next");
    }

}
