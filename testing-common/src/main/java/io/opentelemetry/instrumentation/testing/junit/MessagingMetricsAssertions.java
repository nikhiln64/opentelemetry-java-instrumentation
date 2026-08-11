/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.testing.junit;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.HashSet;
import java.util.Set;

public final class MessagingMetricsAssertions {

  private static final Set<String> STABLE_METRICS =
      unmodifiableSet(
          new HashSet<>(
              asList(
                  "messaging.client.operation.duration",
                  "messaging.client.sent.messages",
                  "messaging.client.consumed.messages",
                  "messaging.process.duration")));
  private static final Set<String> DEPRECATED_METRICS =
      unmodifiableSet(
          new HashSet<>(
              asList(
                  "messaging.publish.duration",
                  "messaging.receive.duration",
                  "messaging.receive.messages")));

  public static void assertCounter(
      InstrumentationExtension testing,
      String instrumentationName,
      String metricName,
      Attributes attributes) {
    testing.waitAndAssertMetrics(
        instrumentationName,
        metricName,
        metrics -> metrics.singleElement().satisfies(metric -> verifyCounter(metric, attributes)));
  }

  public static void assertHistogram(
      InstrumentationExtension testing,
      String instrumentationName,
      String metricName,
      Attributes... attributes) {
    testing.waitAndAssertMetrics(
        instrumentationName,
        metricName,
        metrics ->
            metrics.singleElement().satisfies(metric -> verifyHistogram(metric, attributes)));
  }

  public static void assertNoStableMetrics(InstrumentationExtension testing) {
    assertThat(testing.metrics()).noneMatch(metric -> STABLE_METRICS.contains(metric.getName()));
  }

  public static void assertNoMetric(
      InstrumentationExtension testing, String instrumentationName, String metricName) {
    assertThat(testing.metrics())
        .noneMatch(
            metric ->
                metric.getInstrumentationScopeInfo().getName().equals(instrumentationName)
                    && metric.getName().equals(metricName));
  }

  public static void assertNoDeprecatedMetrics(InstrumentationExtension testing) {
    assertThat(testing.metrics())
        .noneMatch(metric -> DEPRECATED_METRICS.contains(metric.getName()));
  }

  private static void verifyCounter(MetricData metric, Attributes attributes) {
    assertThat(metric.getLongSumData().getPoints())
        .singleElement()
        .satisfies(
            point -> {
              assertThat(point.getValue()).isEqualTo(1);
              assertThat(point.getAttributes()).isEqualTo(attributes);
            });
  }

  private static void verifyHistogram(MetricData metric, Attributes... attributes) {
    assertThat(metric.getHistogramData().getPoints())
        .allSatisfy(point -> assertThat(point.getCount()).isEqualTo(1))
        .extracting(point -> point.getAttributes())
        .containsExactlyInAnyOrder(attributes);
  }

  private MessagingMetricsAssertions() {}
}
