/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.vertx.kafkaclient.v3_6;

import static io.opentelemetry.javaagent.instrumentation.vertx.kafkaclient.v3_6.VertxKafkaSingletons.RECORD_HANDLER_REGISTERED;
import static io.opentelemetry.javaagent.instrumentation.vertx.kafkaclient.v3_6.VertxKafkaSingletons.batchProcessInstrumenter;
import static io.opentelemetry.javaagent.instrumentation.vertx.kafkaclient.v3_6.VertxKafkaSingletons.batchProcessWithConsumedMessagesInstrumenter;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaConsumerContext;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaConsumerContextUtil;
import io.opentelemetry.instrumentation.kafkaclients.common.v0_11.internal.KafkaReceiveRequest;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import io.vertx.core.Handler;
import io.vertx.kafka.client.consumer.impl.KafkaReadStreamImpl;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class InstrumentedBatchRecordsHandler<K, V> implements Handler<ConsumerRecords<K, V>> {

  private final KafkaReadStreamImpl<K, V> readStream;
  @Nullable private final Handler<ConsumerRecords<K, V>> delegate;

  public InstrumentedBatchRecordsHandler(
      KafkaReadStreamImpl<K, V> readStream, @Nullable Handler<ConsumerRecords<K, V>> delegate) {
    this.readStream = readStream;
    this.delegate = delegate;
  }

  @Override
  public void handle(ConsumerRecords<K, V> records) {
    KafkaConsumerContext consumerContext = KafkaConsumerContextUtil.get(records);
    Context receiveContext = consumerContext.getContext();
    // use the receive CONSUMER span as parent if it's available
    Context parentContext = receiveContext != null ? receiveContext : Context.current();

    Instrumenter<KafkaReceiveRequest, Void> instrumenter = instrumenter();
    KafkaReceiveRequest request = KafkaReceiveRequest.create(consumerContext, records);
    if (!instrumenter.shouldStart(parentContext, request)) {
      callDelegateHandler(records);
      return;
    }

    // the instrumenter iterates over records when adding links, we need to suppress that
    boolean previousWrappingEnabled = KafkaClientsConsumerProcessTracing.setWrappingEnabled(false);
    try {
      Context context = instrumenter.start(parentContext, request);
      try (Scope ignored = context.makeCurrent()) {
        callDelegateHandler(records);
      } catch (Throwable t) {
        instrumenter.end(context, request, null, t);
        throw t;
      }
      instrumenter.end(context, request, null, null);
    } finally {
      KafkaClientsConsumerProcessTracing.setWrappingEnabled(previousWrappingEnabled);
    }
  }

  // when the application also registered a per-record handler, the per-record process operation
  // records the consumed messages count; when it didn't, this batch is the only process operation
  // and has to record that count itself
  private Instrumenter<KafkaReceiveRequest, Void> instrumenter() {
    return Boolean.TRUE.equals(RECORD_HANDLER_REGISTERED.get(readStream))
        ? batchProcessInstrumenter()
        : batchProcessWithConsumedMessagesInstrumenter();
  }

  private void callDelegateHandler(ConsumerRecords<K, V> records) {
    if (delegate != null) {
      delegate.handle(records);
    }
  }
}
