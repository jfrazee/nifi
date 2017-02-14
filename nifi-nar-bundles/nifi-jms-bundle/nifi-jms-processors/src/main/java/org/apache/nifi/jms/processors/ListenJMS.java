package org.apache.nifi.jms.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.jms.listener.DefaultMessageListenerContainer;


@TriggerSerially
@Tags({ "jms", "get", "message", "receive", "consume" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Listens and consumes JMS Message of type BytesMessage or TextMessage transforming their content to "
        + "a FlowFile and transitioning them to 'success' relationship. JMS attributes such as headers and properties are copied as FlowFile attributes. "
        + "This processor starts embedded JMS Broker (using Apache ActiveMQ) and MessageListener that will listen on the P2P (i.e., QUEUE) destination"
        + " named 'queue://dest-[PORT]' (e.g., 'queue://dest-61616'), so any messages that are sent by the external producer will be consumed by this processor.")
public class ListenJMS extends AbstractProcessor {

    public static final PropertyDescriptor BROKER_URI = new PropertyDescriptor.Builder()
            .name("broker-uri")
            .displayName("Broker URI")
            .description("The message broker URI. The message broker will bind to the IP address and port in the URI.")
            .defaultValue("tcp://localhost:61616")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONCURRENT_CONSUMERS = new PropertyDescriptor.Builder()
            .name("concurrent.consumers")
            .displayName("Concurrent Consumers")
            .description("Specifies the number of concurrent consumers to create. Default is 1. If specified value is > 1, then the ordering of messages will be affected.")
            .defaultValue("1")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Message Queue")
            .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
                    "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
                    "memory used by the processor.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All received messages that are successfully written as FlowFiles are routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> descriptors;

    private final static Set<Relationship> relationships;

    static {
        descriptors = new ArrayList<>();
        descriptors.add(BROKER_URI);
        descriptors.add(CONCURRENT_CONSUMERS);
        descriptors.add(MAX_MESSAGE_QUEUE_SIZE);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
    }

    private volatile BrokerService broker;

    private volatile DefaultMessageListenerContainer messageListenerContainer;

    private volatile String transitUri;

    private BlockingQueue<Message> messages;

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (!broker.isStarted() || !messageListenerContainer.isRunning()) {
            this.start(context);
        }

        FlowFile flowFile = session.create();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        Message msg = null;
        try {
            msg = messages.take();
        } catch (InterruptedException ie) {
            throw new ProcessException(ie.getMessage(), ie);
        }
        final Message message = msg;

        byte[] mBody = null;
        if (message instanceof TextMessage) {
            mBody = MessageBodyToBytesConverter.toBytes((TextMessage) message);
        } else if (message instanceof BytesMessage) {
            mBody = MessageBodyToBytesConverter.toBytes((BytesMessage) message);
        } else {
            throw new IllegalStateException("Message type other then TextMessage and BytesMessage are not supported.");
        }
        final byte[] messageBody = mBody;

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(messageBody);
            }
        });

        if (logger.isDebugEnabled()) {
            logger.debug("Created FlowFile {} from Message {}", new Object[]{flowFile, messageBody});
        }

        // add attributes (headers and properties) and properties to FF
        Map<String, Object> headers = JMSUtils.extractMessageHeaders(message);
        flowFile = updateFlowFileAttributesWithJMSAttributes(headers, flowFile, session);
        Map<String, String> properties = JMSUtils.extractMessageProperties(message);
        flowFile = updateFlowFileAttributesWithJMSAttributes(properties, flowFile, session);

        session.transfer(flowFile, REL_SUCCESS);
        session.getProvenanceReporter().receive(flowFile, transitUri);
        session.commit();
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(relationships);
    }

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(descriptors);
    }

    /**
     *
     */
    @OnScheduled
    public void initialize(ProcessContext processContext) throws Exception {
        final URI uri =
            new URI(processContext.getProperty(BROKER_URI).getValue());
        final int concurrentConsumers = processContext.getProperty(CONCURRENT_CONSUMERS).asInteger();
        final int maxQueueSize = processContext.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger();
        final String destinationName = "dest-" + uri.getPort();

        this.messages = new LinkedBlockingQueue<>(maxQueueSize);

        this.broker = new BrokerService();
        broker.addConnector("vm://localhost");
        broker.addConnector(uri);
        broker.setPersistent(false);
        broker.setUseJmx(false);

        final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uri);

        this.messageListenerContainer = new DefaultMessageListenerContainer();
        messageListenerContainer.setConnectionFactory(connectionFactory);
        messageListenerContainer.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        messageListenerContainer.setConcurrentConsumers(concurrentConsumers);
        messageListenerContainer.setDestinationName(destinationName);
        messageListenerContainer.setMessageListener(new InProcessMessageListener(messages));
        messageListenerContainer.setAutoStartup(false);
        messageListenerContainer.afterPropertiesSet();

        this.transitUri = uri.resolve("/").resolve(destinationName).toString();
    }

    /**
     *
     */
    private void start(ProcessContext processContext) {
        final ComponentLog logger = getLogger();

        if (!broker.isStarted()) {
            try {
                broker.start();
            } catch (Exception e) {
                throw new ProcessException("Failed to start message broker", e);
            }
        } else {
            logger.warn("Message broker was already started");
        }

        if (!broker.isStarted()) {
            throw new ProcessException("Failed to start message broker");
        }

        if (!messageListenerContainer.isRunning()) {
            try {
                messageListenerContainer.start();
            } catch (Exception e) {
                throw new ProcessException("Failed to start message listener container");
            }
        } else {
            logger.warn("Message listener container was already running");
        }

        if (!messageListenerContainer.isRunning()) {
            throw new ProcessException("Failed to start message listener container");
        }
    }

    /**
     * Will stop the Message Broker so no new messages are accepted
     */
    @OnStopped
    public void stop() {
        final ComponentLog logger = getLogger();

        if (broker.isStarted()) {
            try {
                broker.stop();
            } catch (Exception e) {
                logger.error("Failed to stop message broker", e);
            }
        } else {
            logger.warn("Message broker was not started");
        }

        if (broker.isStarted()) {
            throw new ProcessException("Failed to stop message broker");
        }

        if (messageListenerContainer.isRunning()) {
            try {
                messageListenerContainer.stop();
            } catch (Exception e) {
                logger.error("Failed to stop message listener container", e);
            }
        } else {
            logger.warn("Message listener container was not running");
        }

        if (messageListenerContainer.isRunning()) {
            throw new ProcessException("Failed to stop message listener container");
        }
    }

    /**
     * Implementation of the {@link MessageListener} to de-queue messages from
     * the target P2P {@link Destination}.
     */
    private class InProcessMessageListener implements MessageListener {

        private BlockingQueue<Message> messages;

        public InProcessMessageListener(BlockingQueue<Message> messages) {
            this.messages = messages;
        }

        /**
         * Given that we set ACK mode as CLIENT, the message will be
         * auto-acknowledged by the MessageListenerContainer upon successful
         * execution of onMessage() operation, otherwise
         * MessageListenerContainer will issue session.recover() call to allow
         * message to remain on the queue.
         */
        @Override
        public void onMessage(Message message) {
            final ComponentLog logger = getLogger();

            if (logger.isDebugEnabled()) {
                logger.debug("Received Message {}", new Object[]{message});
            }

            try {
                messages.put(message);
            } catch (InterruptedException ie) {
                logger.error("Error while processing message: {}", new Object[]{ie.getMessage()}, ie);
                throw new RuntimeException(ie);
            } catch (Exception e) {
                logger.error("Error while processing message: {}", new Object[]{e.getMessage()}, e);
                throw e;
            }
        }
    }

    /**
     * Copies JMS attributes (i.e., headers and properties) as FF attributes.
     * Given that FF attributes mandate that values are of type String, the
     * copied values of JMS attributes will be stringified via
     * String.valueOf(attribute).
     */
    private FlowFile updateFlowFileAttributesWithJMSAttributes(Map<String, ? extends Object> jmsAttributes, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<String, String>();
        for (Entry<String, ? extends Object> entry : jmsAttributes.entrySet()) {
            attributes.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }
}
