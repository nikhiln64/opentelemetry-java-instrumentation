/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.vertx.kafkaclient.v3_6;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.incubator.config.internal.DeclarativeConfigUtil;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaInstrumenterFactory;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaProcessRequest;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaReceiveRequest;
import io.opentelemetry.javaagent.bootstrap.internal.ExperimentalConfig;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;

public class VertxKafkaSingletons {

  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.vertx-kafka-client-3.6";

  /**
   * Whether the application has registered a per-record handler on a given read stream. A read
   * stream can be consumed with only a batch handler, in which case no per-record process spans are
   * created and the batch process operation has to record the consumed messages count itself.
   */
  public static final VirtualField<KafkaReadStreamImpl<?, ?>, Boolean> RECORD_HANDLER_REGISTERED =
      VirtualField.find(KafkaReadStreamImpl.class, Boolean.class);

  private static final Instrumenter<KafkaReceiveRequest, Void> batchProcessInstrumenter;
  private static final Instrumenter<KafkaReceiveRequest, Void>
      batchProcessWithConsumedMessagesInstrumenter;
  private static final Instrumenter<KafkaProcessRequest, Void> processInstrumenter;

  static {
    KafkaInstrumenterFactory factory =
        new KafkaInstrumenterFactory(GlobalOpenTelemetry.get(), INSTRUMENTATION_NAME)
            .setCapturedHeaders(ExperimentalConfig.get().getMessagingHeaders())
            .setCaptureExperimentalSpanAttributes(
                DeclarativeConfigUtil.getInstrumentationConfig(GlobalOpenTelemetry.get(), "kafka")
                    .getBoolean("experimental_span_attributes/development", false));
    Boolean receiveTelemetryEnabled =
        ExperimentalConfig.get().messagingReceiveInstrumentationEnabled();
    if (receiveTelemetryEnabled != null) {
      factory.setMessagingReceiveTelemetryEnabled(receiveTelemetryEnabled);
    }
    // when a per-record handler is registered the per-record process operation records the consumed
    // messages count, so the batch process operation must not record it again
    batchProcessInstrumenter = factory.createBatchProcessInstrumenter(false);
    batchProcessWithConsumedMessagesInstrumenter = factory.createBatchProcessInstrumenter();
    processInstrumenter = factory.createConsumerProcessInstrumenter();
  }

  public static Instrumenter<KafkaReceiveRequest, Void> batchProcessInstrumenter() {
    return batchProcessInstrumenter;
  }

  public static Instrumenter<KafkaReceiveRequest, Void>
      batchProcessWithConsumedMessagesInstrumenter() {
    return batchProcessWithConsumedMessagesInstrumenter;
  }

  public static Instrumenter<KafkaProcessRequest, Void> processInstrumenter() {
    return processInstrumenter;
  }

  private VertxKafkaSingletons() {}
}
