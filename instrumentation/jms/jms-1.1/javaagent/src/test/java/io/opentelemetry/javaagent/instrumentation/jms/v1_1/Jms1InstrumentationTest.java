/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v1_1;

import static io.opentelemetry.api.trace.SpanKind.CLIENT;
import static io.opentelemetry.api.trace.SpanKind.CONSUMER;
import static io.opentelemetry.api.trace.SpanKind.PRODUCER;
import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertCounter;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertHistogram;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoDeprecatedMetrics;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoMetric;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static io.opentelemetry.semconv.ErrorAttributes.ERROR_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_MESSAGE_ID;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class Jms1InstrumentationTest extends AbstractJms1Test {

  @SuppressWarnings("deprecation") // using deprecated semconv
  @ParameterizedTest
  @MethodSource("destinationArguments")
  void testMessageConsumer(
      DestinationFactory destinationFactory, String destinationName, boolean isTemporary)
      throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);
    TextMessage sentMessage = session.createTextMessage("a message");

    MessageProducer producer = session.createProducer(destination);
    cleanup.deferCleanup(producer::close);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer::close);

    // when
    testing.runWithSpan("producer parent", () -> producer.send(sentMessage));

    TextMessage receivedMessage =
        testing.runWithSpan("consumer parent", () -> (TextMessage) consumer.receive());

    // then
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String messageId = receivedMessage.getJMSMessageID();

    AtomicReference<SpanData> producerSpan = new AtomicReference<>();
    testing.waitAndAssertTraces(
        trace -> {
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
                          messagingTempDestination(isTemporary)));

          producerSpan.set(trace.getSpan(1));
        },
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("consumer parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? destinationName.equals("(temporary)")
                                    ? "receive"
                                    : "receive " + destinationName
                                : destinationName + " receive")
                        .hasKind(emitStableMessagingSemconv() ? CLIENT : CONSUMER)
                        .hasParent(trace.getSpan(0))
                        .hasLinks(LinkData.create(producerSpan.get().getSpanContext()))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(destinationName, isTemporary),
                            oldOperation("receive"),
                            operationName("receive"),
                            operationType("receive"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(isTemporary))));
    assertProducerAndReceiveMetrics(testing, destinationName, isTemporary);
  }

  @Test
  void failedSendRecordsAttempt() throws JMSException {
    assumeTrue(emitStableMessagingSemconv());
    Destination destination = session.createQueue("failedSend");
    TextMessage message = session.createTextMessage("test");
    message.setJMSDestination(destination);
    MessageProducer producer = session.createProducer(destination);
    producer.close();

    Throwable failure = catchThrowable(() -> producer.send(message));

    assertThat(failure).isInstanceOf(JMSException.class);
    Attributes attributes = failureMetricAttributes("send", "failedSend", failure, false);
    assertCounter(
        testing, "io.opentelemetry.jms-1.1", "messaging.client.sent.messages", attributes);
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        failureMetricAttributes("send", "failedSend", failure, true));
    assertNoDeprecatedMetrics(testing);
  }

  @Test
  void failedListenerRecordsDeliveredMessage() throws JMSException {
    assumeTrue(emitStableMessagingSemconv());
    Destination destination = session.createQueue("failedListener");
    TextMessage message = session.createTextMessage("test");
    message.setJMSDestination(destination);
    MessageListener listener = new FailingListener();

    Throwable failure = catchThrowable(() -> listener.onMessage(message));

    assertThat(failure).isInstanceOf(IllegalStateException.class);
    Attributes attributes = failureMetricAttributes("process", "failedListener", failure, false);
    assertCounter(
        testing, "io.opentelemetry.jms-1.1", "messaging.client.consumed.messages", attributes);
    assertHistogram(testing, "io.opentelemetry.jms-1.1", "messaging.process.duration", attributes);
    assertNoDeprecatedMetrics(testing);
  }

  @Test
  void failedReceiveRecordsAttempt() throws JMSException {
    assumeTrue(emitStableMessagingSemconv());
    MessageConsumer consumer = session.createConsumer(session.createQueue("failedReceive"));
    consumer.close();

    Throwable failure = catchThrowable(consumer::receiveNoWait);

    assertThat(failure).isInstanceOf(JMSException.class);
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        failureMetricAttributes("receive", failure, true));
    assertNoMetric(testing, "io.opentelemetry.jms-1.1", "messaging.client.consumed.messages");
    assertNoDeprecatedMetrics(testing);
  }

  private static Attributes failureMetricAttributes(
      String operation, String destination, Throwable failure, boolean includeOperationType) {
    AttributesBuilder builder =
        Attributes.builder()
            .put(MESSAGING_OPERATION_NAME, operation)
            .put(MESSAGING_SYSTEM, "jms")
            .put(MESSAGING_DESTINATION_NAME, destination)
            .put(ERROR_TYPE, failure.getClass().getName());
    if (includeOperationType) {
      builder.put(MESSAGING_OPERATION_TYPE, operation);
    }
    return builder.build();
  }

  private static Attributes failureMetricAttributes(
      String operation, Throwable failure, boolean includeOperationType) {
    AttributesBuilder builder =
        Attributes.builder()
            .put(MESSAGING_OPERATION_NAME, operation)
            .put(MESSAGING_SYSTEM, "jms")
            .put(ERROR_TYPE, failure.getClass().getName());
    if (includeOperationType) {
      builder.put(MESSAGING_OPERATION_TYPE, operation);
    }
    return builder.build();
  }

  static class FailingListener implements MessageListener {
    @Override
    public void onMessage(Message message) {
      throw new IllegalStateException("test");
    }
  }
}
