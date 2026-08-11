/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rabbitmq.v2_7;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;

@EnabledIfSystemProperty(
    named = "otel.instrumentation.messaging.experimental.receive-telemetry.enabled",
    matches = "false")
class RabbitMqReceiveDisabledTest extends AbstractRabbitMqTest {

  @RegisterExtension
  private static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  private Connection connection;
  private Channel channel;

  @BeforeEach
  void setUp() throws IOException, TimeoutException {
    connection = connectionFactory.newConnection();
    channel = connection.createChannel();
  }

  @AfterEach
  void tearDown() throws IOException, TimeoutException {
    channel.close();
    connection.close();
  }

  @Test
  void emptyReceiveEmitsNoTelemetry() throws IOException {
    String queue = channel.queueDeclare().getQueue();
    testing.clearData();

    GetResponse response = channel.basicGet(queue, true);

    assertThat(response).isNull();
    assertThat(testing.spans()).isEmpty();
    RabbitMqMetricsAssertions.assertNoMessagingMetrics(testing);
  }
}
