/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v1_1;

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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTextMessage;
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

@SuppressWarnings("deprecation") // using deprecated semconv
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractJms1Test {
  private static final Logger logger = LoggerFactory.getLogger(AbstractJms1Test.class);

  @RegisterExtension
  static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @RegisterExtension final AutoCleanupExtension cleanup = AutoCleanupExtension.create();

  Session session;

  @BeforeAll
  void setUp() throws JMSException {
    GenericContainer<?> broker =
        new GenericContainer<>("apache/activemq-classic:5.19.2")
            .withExposedPorts(61616, 8161)
            .withLogConsumer(new Slf4jLogConsumer(logger));
    broker.start();
    cleanup.deferAfterAll(broker);

    ActiveMQConnectionFactory connectionFactory =
        new ActiveMQConnectionFactory(
            "tcp://" + broker.getHost() + ":" + broker.getMappedPort(61616));
    Connection connection = connectionFactory.createConnection();
    connection.start();
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    cleanup.deferAfterAll(connection::close);
    cleanup.deferAfterAll(session::close);
  }

  @ParameterizedTest
  @MethodSource("destinationArguments")
  void testMessageListener(
      DestinationFactory destinationFactory, String destinationName, boolean isTemporary)
      throws Exception {

    // given
    Destination destination = destinationFactory.create(session);
    TextMessage sentMessage = session.createTextMessage("a message");

    MessageProducer producer = session.createProducer(null);
    cleanup.deferCleanup(producer::close);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer::close);

    CompletableFuture<TextMessage> receivedMessageFuture = new CompletableFuture<>();
    consumer.setMessageListener(
        message ->
            testing.runWithSpan(
                "consumer", () -> receivedMessageFuture.complete((TextMessage) message)));

    // when
    testing.runWithSpan("producer parent", () -> producer.send(destination, sentMessage));

    // then
    TextMessage receivedMessage = receivedMessageFuture.get(10, SECONDS);
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String messageId = receivedMessage.getJMSMessageID();

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("producer parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? destinationName.equals("(temporary)")
                                    ? "send"
                                    : "send " + destinationName
                                : destinationName + " publish")
                        .hasKind(PRODUCER)
                        .hasParent(trace.getSpan(0))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
                            oldOperation("publish"),
                            operationName("send"),
                            operationType("send"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary)),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? destinationName.equals("(temporary)")
                                    ? "process"
                                    : "process " + destinationName
                                : destinationName + " process")
                        .hasKind(CONSUMER)
                        .hasParent(trace.getSpan(1))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary)),
                span -> span.hasName("consumer").hasParent(trace.getSpan(2))));
    assertProducerAndProcessMetrics(testing, destinationName, isTemporary);
  }

  @ParameterizedTest
  @MethodSource("emptyReceiveArguments")
  void shouldEmitReceiveTelemetryOnEmptyReceive(
      DestinationFactory destinationFactory, MessageReceiver receiver) throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);

    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer::close);

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
    assertReceiveMetrics(testing, null, false);
  }

  @ParameterizedTest
  @MethodSource("destinationArguments")
  void shouldCaptureMessageHeaders(
      DestinationFactory destinationFactory, String destinationName, boolean isTemporary)
      throws Exception {

    // given
    Destination destination = destinationFactory.create(session);
    TextMessage sentMessage = session.createTextMessage("a message");
    sentMessage.setStringProperty("Test_Message_Header", "test");
    sentMessage.setStringProperty("Uncaptured_Header", "password");
    sentMessage.setIntProperty("Test_Message_Int_Header", 1234);

    MessageProducer producer = session.createProducer(destination);
    cleanup.deferCleanup(producer::close);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer::close);

    CompletableFuture<TextMessage> receivedMessageFuture = new CompletableFuture<>();
    consumer.setMessageListener(
        message ->
            testing.runWithSpan(
                "consumer", () -> receivedMessageFuture.complete((TextMessage) message)));

    // when
    testing.runWithSpan("producer parent", () -> producer.send(sentMessage));

    // then
    TextMessage receivedMessage = receivedMessageFuture.get(10, SECONDS);
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String messageId = receivedMessage.getJMSMessageID();

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("producer parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? destinationName.equals("(temporary)")
                                    ? "send"
                                    : "send " + destinationName
                                : destinationName + " publish")
                        .hasKind(PRODUCER)
                        .hasParent(trace.getSpan(0))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
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
                                ? destinationName.equals("(temporary)")
                                    ? "process"
                                    : "process " + destinationName
                                : destinationName + " process")
                        .hasKind(CONSUMER)
                        .hasParent(trace.getSpan(1))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary),
                            equalTo(
                                stringArrayKey("messaging.header.Test_Message_Header"),
                                singletonList("test")),
                            equalTo(
                                stringArrayKey("messaging.header.Test_Message_Int_Header"),
                                singletonList("1234"))),
                span -> span.hasName("consumer").hasParent(trace.getSpan(2))));
  }

  @ParameterizedTest
  @MethodSource("destinationArguments")
  void shouldFailWhenSendingReadOnlyMessage(
      DestinationFactory destinationFactory, String destinationName, boolean isTemporary)
      throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);
    ActiveMQTextMessage sentMessage = (ActiveMQTextMessage) session.createTextMessage("a message");

    MessageProducer producer = session.createProducer(destination);
    cleanup.deferCleanup(producer::close);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer::close);

    sentMessage.setReadOnlyProperties(true);

    // when
    testing.runWithSpan("producer parent", () -> producer.send(sentMessage));

    TextMessage receivedMessage = (TextMessage) consumer.receive();

    // then
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String messageId = receivedMessage.getJMSMessageID();

    // This will result in a logged failure because we tried to
    // write properties in MessagePropertyTextMap when readOnlyProperties = true.
    // As a result, the consumer span will not be linked to the producer span as we are unable to
    // propagate the trace context as a message property.
    if (!receiveTelemetryEnabled()) {
      testing.waitAndAssertTraces(
          trace ->
              trace.hasSpansSatisfyingExactly(
                  span -> span.hasName("producer parent").hasNoParent(),
                  span ->
                      span.hasName(
                              emitStableMessagingSemconv()
                                  ? destinationName.equals("(temporary)")
                                      ? "send"
                                      : "send " + destinationName
                                  : destinationName + " publish")
                          .hasKind(PRODUCER)
                          .hasParent(trace.getSpan(0))
                          .hasAttributesSatisfyingExactly(
                              equalTo(MESSAGING_SYSTEM, "jms"),
                              messagingDestinationName(destinationName, isTemporary),
                              oldOperation("publish"),
                              operationName("send"),
                              operationType("send"),
                              equalTo(MESSAGING_MESSAGE_ID, messageId),
                              messagingTempDestination(isTemporary))));
      assertProducerMetrics(testing, destinationName, isTemporary);
      return;
    }

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("producer parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? destinationName.equals("(temporary)")
                                    ? "send"
                                    : "send " + destinationName
                                : destinationName + " publish")
                        .hasKind(PRODUCER)
                        .hasParent(trace.getSpan(0))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
                            oldOperation("publish"),
                            operationName("send"),
                            operationType("send"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary))),
        trace ->
            trace.hasSpansSatisfyingExactly(
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? destinationName.equals("(temporary)")
                                    ? "receive"
                                    : "receive " + destinationName
                                : destinationName + " receive")
                        .hasKind(emitStableMessagingSemconv() ? CLIENT : CONSUMER)
                        .hasNoParent()
                        .hasTotalRecordedLinks(0)
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
                            oldOperation("receive"),
                            operationName("receive"),
                            operationType("receive"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary))));
  }

  static AttributeAssertion messagingTempDestination(boolean isTemporary) {
    return isTemporary
        ? equalTo(MESSAGING_DESTINATION_TEMPORARY, true)
        : satisfies(MESSAGING_DESTINATION_TEMPORARY, AbstractAssert::isNull);
  }

  static AttributeAssertion messagingDestinationName(String destinationName, boolean isTemporary) {
    return emitStableMessagingSemconv() && isTemporary
        ? satisfies(MESSAGING_DESTINATION_NAME, val -> val.isNotEmpty())
        : equalTo(MESSAGING_DESTINATION_NAME, destinationName);
  }

  static AttributeAssertion oldOperation(String operation) {
    return equalTo(MESSAGING_OPERATION, emitOldMessagingSemconv() ? operation : null);
  }

  private static AttributeAssertion emptyBatchMessageCount() {
    return satisfies(
        MESSAGING_BATCH_MESSAGE_COUNT,
        val -> {
          if (emitStableMessagingSemconv()) {
            val.isZero();
          } else {
            val.isNull();
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
      InstrumentationExtension testing, String destinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertCounter(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.sent.messages",
        metricAttributes("send", destinationName, isTemporary, false));
    assertCounter(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.consumed.messages",
        metricAttributes("receive", destinationName, isTemporary, false));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        metricAttributes("send", destinationName, isTemporary, true),
        metricAttributes("receive", destinationName, isTemporary, true));
    assertNoDeprecatedMetrics(testing);
  }

  static void assertProducerMetrics(
      InstrumentationExtension testing, String destinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertCounter(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.sent.messages",
        metricAttributes("send", destinationName, isTemporary, false));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        metricAttributes("send", destinationName, isTemporary, true));
    assertNoDeprecatedMetrics(testing);
  }

  static void assertProducerAndProcessMetrics(
      InstrumentationExtension testing, String destinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertCounter(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.sent.messages",
        metricAttributes("send", destinationName, isTemporary, false));
    assertCounter(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.consumed.messages",
        metricAttributes("process", destinationName, isTemporary, false));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        metricAttributes("send", destinationName, isTemporary, true));
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.process.duration",
        metricAttributes("process", destinationName, isTemporary, false));
    assertNoDeprecatedMetrics(testing);
  }

  static void assertReceiveMetrics(
      InstrumentationExtension testing, String destinationName, boolean isTemporary) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        metricAttributes("receive", destinationName, isTemporary, true));
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

  protected static Stream<Arguments> destinationArguments() {
    DestinationFactory topic = session -> session.createTopic("someTopic");
    DestinationFactory queue = session -> session.createQueue("someQueue");
    DestinationFactory tempTopic = Session::createTemporaryTopic;
    DestinationFactory tempQueue = Session::createTemporaryQueue;

    return Stream.of(
        arguments(topic, "someTopic", false),
        arguments(queue, "someQueue", false),
        arguments(tempTopic, "(temporary)", true),
        arguments(tempQueue, "(temporary)", true));
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
