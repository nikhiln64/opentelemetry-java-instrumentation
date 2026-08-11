/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.spring.jms.v6_0;

import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertCounter;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertHistogram;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoDeprecatedMetrics;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoMetric;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoStableMetrics;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.instrumentation.testing.internal.AutoCleanupExtension;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import jakarta.jms.ConnectionFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jms.core.JmsTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

abstract class AbstractSpringJmsListenerTest {
  private static final Logger logger = LoggerFactory.getLogger(AbstractSpringJmsListenerTest.class);

  @RegisterExtension
  static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @RegisterExtension static final AutoCleanupExtension cleanup = AutoCleanupExtension.create();

  private static GenericContainer<?> broker;

  @BeforeAll
  static void setUp() {
    broker =
        new GenericContainer<>("apache/activemq-artemis:2.44.0")
            .withEnv("ARTEMIS_USER", "test")
            .withEnv("ARTEMIS_PASSWORD", "test")
            .withEnv("JAVA_TOOL_OPTIONS", "-Dbrokerconfig.maxDiskUsage=-1")
            .withExposedPorts(61616, 8161)
            .waitingFor(Wait.forLogMessage(".*Server is now active.*", 1))
            .withStartupTimeout(Duration.ofMinutes(2))
            .withLogConsumer(new Slf4jLogConsumer(logger));
    broker.start();
    cleanup.deferAfterAll(broker);
  }

  @ParameterizedTest
  @ValueSource(classes = {AnnotatedListenerConfig.class, ManualListenerConfig.class})
  @SuppressWarnings("unchecked")
  void testSpringJmsListener(Class<?> configClass) throws Exception {
    // given
    SpringApplication app = new SpringApplication(configClass);
    app.setDefaultProperties(defaultConfig());
    ConfigurableApplicationContext applicationContext = app.run();
    cleanup.deferCleanup(applicationContext);

    JmsTemplate jmsTemplate = new JmsTemplate(applicationContext.getBean(ConnectionFactory.class));
    String message = "hello there";

    // when
    testing.runWithSpan("parent", () -> jmsTemplate.convertAndSend("spring-jms-listener", message));

    // then
    CompletableFuture<String> receivedMessage =
        applicationContext.getBean("receivedMessage", CompletableFuture.class);
    assertThat(receivedMessage.get(10, SECONDS)).isEqualTo(message);

    assertSpringJmsListener();
  }

  abstract void assertSpringJmsListener();

  static void assertMetrics(boolean receiveTelemetryEnabled) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }

    Attributes sendAttributes = metricAttributes("send", false);
    Attributes receiveAttributes = metricAttributes("receive", false);
    Attributes processAttributes = metricAttributes("process", false);
    assertCounter(
        testing, "io.opentelemetry.jms-3.0", "messaging.client.sent.messages", sendAttributes);
    assertHistogram(
        testing,
        "io.opentelemetry.spring-jms-6.0",
        "messaging.process.duration",
        processAttributes);
    if (receiveTelemetryEnabled) {
      assertCounter(
          testing,
          "io.opentelemetry.jms-3.0",
          "messaging.client.consumed.messages",
          receiveAttributes);
      assertHistogram(
          testing,
          "io.opentelemetry.jms-3.0",
          "messaging.client.operation.duration",
          metricAttributes("send", true),
          metricAttributes("receive", true));
      assertNoMetric(
          testing, "io.opentelemetry.spring-jms-6.0", "messaging.client.consumed.messages");
    } else {
      assertCounter(
          testing,
          "io.opentelemetry.spring-jms-6.0",
          "messaging.client.consumed.messages",
          processAttributes);
      assertHistogram(
          testing,
          "io.opentelemetry.jms-3.0",
          "messaging.client.operation.duration",
          metricAttributes("send", true));
      assertNoMetric(testing, "io.opentelemetry.jms-3.0", "messaging.client.consumed.messages");
    }
    assertNoDeprecatedMetrics(testing);
  }

  private static Attributes metricAttributes(String operation, boolean includeOperationType) {
    AttributesBuilder builder =
        Attributes.builder()
            .put(MESSAGING_OPERATION_NAME, operation)
            .put(MESSAGING_SYSTEM, "jms")
            .put(MESSAGING_DESTINATION_NAME, "spring-jms-listener");
    if (includeOperationType) {
      builder.put(MESSAGING_OPERATION_TYPE, operation);
    }
    return builder.build();
  }

  static Map<String, Object> defaultConfig() {
    Map<String, Object> props = new HashMap<>();
    props.put("spring.jmx.enabled", false);
    props.put("spring.main.web-application-type", "none");
    props.put("test.broker-url", "tcp://" + broker.getHost() + ":" + broker.getMappedPort(61616));
    return props;
  }
}
