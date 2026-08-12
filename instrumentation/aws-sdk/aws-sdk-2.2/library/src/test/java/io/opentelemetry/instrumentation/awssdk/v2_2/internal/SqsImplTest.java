/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.awssdk.v2_2.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.internal.Timer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

class SqsImplTest {

  @Test
  void suppressesFailedInternalListenerPollWhenReceiveTelemetryDisabled() {
    ExecutionAttributes executionAttributes = internalListenerPoll();
    TracingExecutionInterceptor config = mock(TracingExecutionInterceptor.class);

    assertThat(
            SqsImpl.afterReceiveMessageExecutionFailure(
                executionAttributes, config, mock(Timer.class), new RuntimeException()))
        .isTrue();

    verify(config, never()).getConsumerReceiveInstrumenter();
  }

  @Test
  @SuppressWarnings("unchecked") // mocking a generic Instrumenter
  void recordsFailedInternalListenerPollWhenReceiveTelemetryEnabled() {
    ExecutionAttributes executionAttributes = internalListenerPoll();
    TracingExecutionInterceptor config = mock(TracingExecutionInterceptor.class);
    Instrumenter<SqsReceiveRequest, Response> instrumenter = mock(Instrumenter.class);
    when(config.isMessagingReceiveInstrumentationExplicitlyEnabled()).thenReturn(true);
    when(config.getConsumerReceiveInstrumenter()).thenReturn(instrumenter);

    assertThat(
            SqsImpl.afterReceiveMessageExecutionFailure(
                executionAttributes, config, mock(Timer.class), new RuntimeException()))
        .isTrue();

    verify(instrumenter).shouldStart(isNull(), any(SqsReceiveRequest.class));
  }

  private static ExecutionAttributes internalListenerPoll() {
    ExecutionAttributes executionAttributes = new ExecutionAttributes();
    executionAttributes.putAttribute(
        TracingExecutionInterceptor.SDK_REQUEST_ATTRIBUTE, ReceiveMessageRequest.builder().build());
    executionAttributes.putAttribute(
        TracingExecutionInterceptor.SQS_INTERNAL_LISTENER_POLL_ATTRIBUTE, true);
    return executionAttributes;
  }
}
