/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.spring.jms.v2_0;

import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.javaagent.instrumentation.spring.jms.v2_0.SpringListenerTest.assertMetrics;

import io.opentelemetry.instrumentation.spring.jms.v2_0.AbstractJmsTest;
import io.opentelemetry.instrumentation.testing.internal.AutoCleanupExtension;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import javax.jms.ConnectionFactory;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jms.core.JmsTemplate;

class SpringListenerSuppressReceiveSpansTest extends AbstractJmsTest {

  @RegisterExtension
  private static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @RegisterExtension
  private static final AutoCleanupExtension cleanup = AutoCleanupExtension.create();

  @ParameterizedTest
  @ValueSource(classes = {AnnotatedListenerConfig.class, PlainListenerConfig.class})
  void receivingMessageInSpringListenerGeneratesSpans(Class<? extends AbstractConfig> config) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(config);
    cleanup.deferCleanup(context);
    ConnectionFactory factory = context.getBean(ConnectionFactory.class);
    JmsTemplate template = new JmsTemplate(factory);

    template.convertAndSend("SpringListenerJms2", "a message");
    if (emitStableMessagingSemconv()) {
      testing.waitAndAssertTraces(
          trace ->
              trace.hasSpansSatisfyingExactly(
                  span -> assertProducerSpan(span, "SpringListenerJms2", false),
                  span ->
                      assertConsumerSpan(
                          span,
                          trace.getSpan(0),
                          trace.getSpan(0),
                          "SpringListenerJms2",
                          "process",
                          false,
                          null)));
      assertMetrics(
          testing,
          false,
          config == PlainListenerConfig.class
              ? "io.opentelemetry.jms-1.1"
              : "io.opentelemetry.spring-jms-2.0");
      return;
    }
    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> assertProducerSpan(span, "SpringListenerJms2", false),
                span ->
                    assertConsumerSpan(
                        span,
                        emitStableMessagingSemconv() ? trace.getSpan(0) : null,
                        trace.getSpan(0),
                        "SpringListenerJms2",
                        "process",
                        false,
                        null)));
    assertMetrics(
        testing,
        false,
        config == PlainListenerConfig.class
            ? "io.opentelemetry.jms-1.1"
            : "io.opentelemetry.spring-jms-2.0");
  }
}
