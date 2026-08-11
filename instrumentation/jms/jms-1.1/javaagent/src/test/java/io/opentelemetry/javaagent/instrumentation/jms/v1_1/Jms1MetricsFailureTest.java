/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v1_1;

import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertCounter;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertHistogram;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoDeprecatedMetrics;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoMetric;
import static io.opentelemetry.semconv.ErrorAttributes.ERROR_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import org.junit.jupiter.api.Test;

class Jms1MetricsFailureTest extends AbstractJms1Test {

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
    Attributes attributes = metricAttributes("send", "failedSend", failure, false);
    assertCounter(
        testing, "io.opentelemetry.jms-1.1", "messaging.client.sent.messages", attributes);
    assertHistogram(
        testing,
        "io.opentelemetry.jms-1.1",
        "messaging.client.operation.duration",
        metricAttributes("send", "failedSend", failure, true));
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
    Attributes attributes = metricAttributes("process", "failedListener", failure, false);
    assertCounter(
        testing, "io.opentelemetry.jms-1.1", "messaging.client.consumed.messages", attributes);
    assertHistogram(testing, "io.opentelemetry.jms-1.1", "messaging.process.duration", attributes);
    assertNoDeprecatedMetrics(testing);
  }

  @Test
  void failedReceiveWithoutRequestDoesNotRecordMetrics() throws JMSException {
    assumeTrue(emitStableMessagingSemconv());
    MessageConsumer consumer = session.createConsumer(session.createQueue("failedReceive"));
    consumer.close();

    Throwable failure = catchThrowable(consumer::receiveNoWait);

    assertThat(failure).isInstanceOf(JMSException.class);
    assertNoMetric(testing, "io.opentelemetry.jms-1.1", "messaging.client.operation.duration");
    assertNoMetric(testing, "io.opentelemetry.jms-1.1", "messaging.client.consumed.messages");
    assertNoDeprecatedMetrics(testing);
  }

  private static Attributes metricAttributes(
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

  static class FailingListener implements MessageListener {
    @Override
    public void onMessage(Message message) {
      throw new IllegalStateException("test");
    }
  }
}
