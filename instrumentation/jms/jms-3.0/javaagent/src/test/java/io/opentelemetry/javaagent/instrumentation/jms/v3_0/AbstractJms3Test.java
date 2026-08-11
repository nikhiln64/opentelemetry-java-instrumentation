/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v3_0;

import static io.opentelemetry.api.common.AttributeKey.stringArrayKey;
import static io.opentelemetry.api.trace.SpanKind.CLIENT;
import static io.opentelemetry.api.trace.SpanKind.CONSUMER;
import static io.opentelemetry.api.trace.SpanKind.PRODUCER;
import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitOldMessagingSemconv;
import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertCounter;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertHistogram;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoDeprecatedMetrics;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoStableMetrics;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.satisfies;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_BATCH_MESSAGE_COUNT;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_TEMPORARY;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_MESSAGE_ID;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.instrumentation.testing.internal.AutoCleanupExtension;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import io.opentelemetry.sdk.testing.assertj.AttributeAssertion;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.assertj.core.api.AbstractAssert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

@SuppressWarnings("deprecation") // using deprecated semconv
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractJms3Test {
  private static final Logger logger = LoggerFactory.getLogger(AbstractJms3Test.class);

  @RegisterExtension
  static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @RegisterExtension final AutoCleanupExtension cleanup = AutoCleanupExtension.create();

  GenericContainer<?> broker;
  ActiveMQConnectionFactory connectionFactory;
  Connection connection;
  Session session;

  @BeforeAll
  void setUp() throws JMSException {
    broker =
        new GenericContainer<>("quay.io/artemiscloud/activemq-artemis-broker:artemis.2.27.0")
            .withEnv("AMQ_USER", "test")
            .withEnv("AMQ_PASSWORD", "test")
            .withEnv("JAVA_TOOL_OPTIONS", "-Dbrokerconfig.maxDiskUsage=-1")
            .withExposedPorts(61616, 8161)
            .waitingFor(Wait.forLogMessage(".*Server is now live.*", 1))
            .withStartupTimeout(Duration.ofMinutes(2))
            .withLogConsumer(new Slf4jLogConsumer(logger));
    broker.start();
    cleanup.deferAfterAll(broker);

    connectionFactory =
        new ActiveMQConnectionFactory(
            "tcp://" + broker.getHost() + ":" + broker.getMappedPort(61616));
    connectionFactory.setUser("test");
    connectionFactory.setPassword("test");

    connection = connectionFactory.createConnection();
    connection.start();

    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    cleanup.deferAfterAll(connectionFactory);
    cleanup.deferAfterAll(connection);
    cleanup.deferAfterAll(session);
  }

  @ParameterizedTest
  @MethodSource("destinationArguments")
  void testMessageListener(DestinationFactory destinationFactory, boolean isTemporary)
      throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);
    TextMessage sentMessage = session.createTextMessage("hello there");

    MessageProducer producer = session.createProducer(null);
    cleanup.deferCleanup(producer);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer);

    CompletableFuture<TextMessage> receivedMessageFuture = new CompletableFuture<>();
    consumer.setMessageListener(
        message ->
            testing.runWithSpan(
                "consumer", () -> receivedMessageFuture.complete((TextMessage) message)));

    // when
    testing.runWithSpan("parent", () -> producer.send(destination, sentMessage));

    // then
    TextMessage receivedMessage = receivedMessageFuture.orTimeout(10, SECONDS).join();
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String actualDestinationName = ((ActiveMQDestination) destination).getName();
    // artemis consumers don't know whether the destination is temporary or not
    String producerDestinationName = isTemporary ? "(temporary)" : actualDestinationName;
    String messageId = receivedMessage.getJMSMessageID();

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? producerDestinationName.equals("(temporary)")
                                    ? "send"
                                    : "send " + producerDestinationName
                                : producerDestinationName + " publish")
                        .hasKind(PRODUCER)
                        .hasParent(trace.getSpan(0))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(
                                producerDestinationName, actualDestinationName),
                            oldOperation("publish"),
                            operationName("send"),
                            operationType("send"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary)),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? actualDestinationName.equals("(temporary)")
                                    ? "process"
                                    : "process " + actualDestinationName
                                : actualDestinationName + " process")
                        .hasKind(CONSUMER)
                        .hasParent(trace.getSpan(1))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            equalTo(MESSAGING_DESTINATION_NAME, actualDestinationName),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId)),
                span -> span.hasName("consumer").hasParent(trace.getSpan(2))));
    assertProducerAndProcessMetrics(producerDestinationName, actualDestinationName, isTemporary);
  }

  @ParameterizedTest
  @MethodSource("emptyReceiveArguments")
  void shouldEmitReceiveTelemetryOnEmptyReceive(
      DestinationFactory destinationFactory, MessageReceiver receiver) throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);

    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer);

    // when
    Message message = receiver.receive(consumer);

    // then
    assertThat(message).isNull();

    if (!receiveTelemetryEnabled()) {
      assertThat(testing.spans()).isEmpty();
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span ->
                    span.hasName(emitStableMessagingSemconv() ? "receive" : "unknown receive")
                        .hasKind(emitStableMessagingSemconv() ? CLIENT : CONSUMER)
                        .hasNoParent()
                        .hasTotalRecordedLinks(0)
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            emptyBatchMessageCount(),
                            oldOperation("receive"),
                            operationName("receive"),
                            operationType("receive"))));
    assertReceiveMetrics(null);
  }

  @ParameterizedTest
  @MethodSource("destinationArguments")
  void shouldCaptureMessageHeaders(DestinationFactory destinationFactory, boolean isTemporary)
      throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);
    TextMessage sentMessage = session.createTextMessage("hello there");
    sentMessage.setStringProperty("Test_Message_Header", "test");
    sentMessage.setStringProperty("Uncaptured_Header", "password");
    sentMessage.setIntProperty("Test_Message_Int_Header", 1234);

    MessageProducer producer = session.createProducer(destination);
    cleanup.deferCleanup(producer);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer);

    CompletableFuture<TextMessage> receivedMessageFuture = new CompletableFuture<>();
    consumer.setMessageListener(
        message ->
            testing.runWithSpan(
                "consumer", () -> receivedMessageFuture.complete((TextMessage) message)));

    // when
    testing.runWithSpan("parent", () -> producer.send(sentMessage));

    // then
    TextMessage receivedMessage = receivedMessageFuture.orTimeout(10, SECONDS).join();
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String actualDestinationName = ((ActiveMQDestination) destination).getName();
    // artemis consumers don't know whether the destination is temporary or not
    String producerDestinationName = isTemporary ? "(temporary)" : actualDestinationName;
    String messageId = receivedMessage.getJMSMessageID();

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? producerDestinationName.equals("(temporary)")
                                    ? "send"
                                    : "send " + producerDestinationName
                                : producerDestinationName + " publish")
                        .hasKind(PRODUCER)
                        .hasParent(trace.getSpan(0))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(
                                producerDestinationName, actualDestinationName),
                            oldOperation("publish"),
                            operationName("send"),
                            operationType("send"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary),
                            equalTo(
                                stringArrayKey("messaging.header.Test_Message_Header"),
                                singletonList("test")),
                            equalTo(
                                stringArrayKey("messaging.header.Test_Message_Int_Header"),
                                singletonList("1234"))),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? actualDestinationName.equals("(temporary)")
                                    ? "process"
                                    : "process " + actualDestinationName
                                : actualDestinationName + " process")
                        .hasKind(CONSUMER)
                        .hasParent(trace.getSpan(1))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            equalTo(MESSAGING_DESTINATION_NAME, actualDestinationName),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            equalTo(
                                stringArrayKey("messaging.header.Test_Message_Header"),
                                singletonList("test")),
                            equalTo(
                                stringArrayKey("messaging.header.Test_Message_Int_Header"),
                                singletonList("1234"))),
                span -> span.hasName("consumer").hasParent(trace.getSpan(2))));
  }

  static AttributeAssertion messagingTempDestination(boolean isTemporary) {
    return isTemporary
        ? equalTo(MESSAGING_DESTINATION_TEMPORARY, true)
        : satisfies(MESSAGING_DESTINATION_TEMPORARY, AbstractAssert::isNull);
  }

  static AttributeAssertion messagingDestinationName(
      String oldDestinationName, String stableDestinationName) {
    return equalTo(
        MESSAGING_DESTINATION_NAME,
        emitStableMessagingSemconv() ? stableDestinationName : oldDestinationName);
  }

  static AttributeAssertion oldOperation(String operation) {
    return equalTo(MESSAGING_OPERATION, emitOldMessagingSemconv() ? operation : null);
  }

  private static AttributeAssertion emptyBatchMessageCount() {
    return satisfies(
        MESSAGING_BATCH_MESSAGE_COUNT,
        value -> {
          if (emitStableMessagingSemconv()) {
            value.isZero();
          } else {
            value.isNull();
          }
        });
  }

  static AttributeAssertion operationName(String operation) {
    return equalTo(MESSAGING_OPERATION_NAME, emitStableMessagingSemconv() ? operation : null);
  }

  static AttributeAssertion operationType(String operation) {
    return equalTo(MESSAGING_OPERATION_TYPE, emitStableMessagingSemconv() ? operation : null);
  }

  static void assertProducerAndReceiveMetrics(
      String producerDestinationName, String consumerDestinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertCounter(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.sent.messages",
        metricAttributes("send", producerDestinationName, isTemporary, false));
    assertCounter(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.consumed.messages",
        metricAttributes("receive", consumerDestinationName, false, false));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.operation.duration",
        metricAttributes("send", producerDestinationName, isTemporary, true),
        metricAttributes("receive", consumerDestinationName, false, true));
    assertNoDeprecatedMetrics(testing);
  }

  static void assertProducerMetrics(String producerDestinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertCounter(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.sent.messages",
        metricAttributes("send", producerDestinationName, isTemporary, false));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.operation.duration",
        metricAttributes("send", producerDestinationName, isTemporary, true));
    assertNoDeprecatedMetrics(testing);
  }

  private static void assertProducerAndProcessMetrics(
      String producerDestinationName, String consumerDestinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertCounter(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.sent.messages",
        metricAttributes("send", producerDestinationName, isTemporary, false));
    assertCounter(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.consumed.messages",
        metricAttributes("process", consumerDestinationName, false, false));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.operation.duration",
        metricAttributes("send", producerDestinationName, isTemporary, true));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.process.duration",
        metricAttributes("process", consumerDestinationName, false, false));
    assertNoDeprecatedMetrics(testing);
  }

  private static void assertReceiveMetrics(String destinationName) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertHistogram(
        testing,
        "io.opentelemetry.jms-3.0",
        "messaging.client.operation.duration",
        metricAttributes("receive", destinationName, false, true));
    assertThat(testing.metrics())
        .noneMatch(metric -> metric.getName().equals("messaging.client.consumed.messages"));
    assertNoDeprecatedMetrics(testing);
  }

  private static Attributes metricAttributes(
      String operation, String destinationName, boolean isTemporary, boolean includeOperationType) {
    AttributesBuilder builder =
        Attributes.builder().put(MESSAGING_OPERATION_NAME, operation).put(MESSAGING_SYSTEM, "jms");
    if (destinationName != null && !isTemporary) {
      builder.put(MESSAGING_DESTINATION_NAME, destinationName);
    }
    if (includeOperationType) {
      builder.put(MESSAGING_OPERATION_TYPE, operation);
    }
    return builder.build();
  }

  private static Stream<Arguments> emptyReceiveArguments() {
    DestinationFactory topic = session -> session.createTopic("someTopic");
    DestinationFactory queue = session -> session.createQueue("someQueue");
    MessageReceiver receive = consumer -> consumer.receive(100);
    MessageReceiver receiveNoWait = MessageConsumer::receiveNoWait;

    return Stream.of(
        arguments(topic, receive),
        arguments(queue, receive),
        arguments(topic, receiveNoWait),
        arguments(queue, receiveNoWait));
  }

  boolean receiveTelemetryEnabled() {
    return true;
  }

  private static Stream<Arguments> destinationArguments() {
    DestinationFactory topic = session -> session.createTopic("someTopic");
    DestinationFactory queue = session -> session.createQueue("someQueue");
    DestinationFactory tempTopic = Session::createTemporaryTopic;
    DestinationFactory tempQueue = Session::createTemporaryQueue;

    return Stream.of(
        arguments(topic, false),
        arguments(queue, false),
        arguments(tempTopic, true),
        arguments(tempQueue, true));
  }

  @FunctionalInterface
  interface DestinationFactory {

    Destination create(Session session) throws JMSException;
  }

  @FunctionalInterface
  interface MessageReceiver {

    Message receive(MessageConsumer consumer) throws JMSException;
  }
}
