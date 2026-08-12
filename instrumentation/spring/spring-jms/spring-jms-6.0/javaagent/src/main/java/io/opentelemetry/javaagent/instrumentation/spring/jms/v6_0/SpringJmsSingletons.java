/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.spring.jms.v6_0;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.javaagent.bootstrap.internal.ExperimentalConfig;
import io.opentelemetry.javaagent.instrumentation.jms.common.v1_1.JmsInstrumenterFactory;
import io.opentelemetry.javaagent.instrumentation.jms.common.v1_1.MessageWithDestination;
import javax.annotation.Nullable;

public class SpringJmsSingletons {
  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.spring-jms-6.0";

  @Nullable
  private static final Boolean receiveTelemetryEnabled =
      ExperimentalConfig.get().messagingReceiveInstrumentationEnabled();

  private static final Instrumenter<MessageWithDestination, Void> listenerInstrumenter;
  private static final Instrumenter<MessageWithDestination, Void> receiveInstrumenter;

  static {
    JmsInstrumenterFactory factory =
        new JmsInstrumenterFactory(GlobalOpenTelemetry.get(), INSTRUMENTATION_NAME)
            .setCapturedHeaders(ExperimentalConfig.get().getMessagingHeaders());
    if (receiveTelemetryEnabled != null) {
      factory.setMessagingReceiveTelemetryEnabled(receiveTelemetryEnabled);
    }

    listenerInstrumenter =
        factory.createConsumerProcessInstrumenter(Boolean.TRUE.equals(receiveTelemetryEnabled));
    receiveInstrumenter = factory.createConsumerReceiveInstrumenter();
  }

  public static Instrumenter<MessageWithDestination, Void> listenerInstrumenter() {
    return listenerInstrumenter;
  }

  public static Instrumenter<MessageWithDestination, Void> receiveInstrumenter() {
    return receiveInstrumenter;
  }

  @Nullable
  public static Boolean receiveTelemetryEnabled() {
    return receiveTelemetryEnabled;
  }

  private SpringJmsSingletons() {}
}
