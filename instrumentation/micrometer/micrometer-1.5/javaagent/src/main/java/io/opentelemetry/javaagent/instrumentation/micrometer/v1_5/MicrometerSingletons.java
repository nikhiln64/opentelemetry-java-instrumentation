/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.micrometer.v1_5;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.incubator.config.DeclarativeConfigProperties;
import io.opentelemetry.instrumentation.api.incubator.config.internal.DeclarativeConfigUtil;
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistry;
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistryBuilder;
import io.opentelemetry.instrumentation.micrometer.v1_5.internal.Experimental;
import io.opentelemetry.instrumentation.micrometer.v1_5.internal.OpenTelemetryInstrument;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

public class MicrometerSingletons {

  private static final MeterRegistry meterRegistry;

  static {
    DeclarativeConfigProperties config =
        DeclarativeConfigUtil.getInstrumentationConfig(GlobalOpenTelemetry.get(), "micrometer");
    OpenTelemetryMeterRegistryBuilder builder =
        OpenTelemetryMeterRegistry.builder(GlobalOpenTelemetry.get())
            .setPrometheusMode(config.get("prometheus_mode").getBoolean("enabled", false))
            .setBaseTimeUnit(TimeUnitParser.parseConfigValue(config.getString("base_time_unit")));
    Experimental.setMicrometerHistogramGaugesEnabled(
        builder, config.get("histogram_gauges").getBoolean("enabled", false));
    meterRegistry = builder.build();
  }

  public static MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  // called from CompositeMeterRegistryInstrumentation
  // the actuator metrics endpoint reads meters from the first registry that has a match, so the
  // otel registry is sorted last since it does not support reading metric values
  public static Set<MeterRegistry> sortOtelMeterRegistryLast(Set<MeterRegistry> registries) {
    if (registries.size() < 2 || !registries.contains(meterRegistry)) {
      return registries;
    }
    Set<MeterRegistry> sorted = new LinkedHashSet<>();
    for (MeterRegistry registry : registries) {
      if (registry != meterRegistry) {
        sorted.add(registry);
      }
    }
    sorted.add(meterRegistry);
    return Collections.unmodifiableSet(sorted);
  }

  // called from code generated in AbstractCompositeMeterInstrumentation
  public static <T> Iterator<T> wrapIterator(Iterator<T> iterator) {
    if (!iterator.hasNext()) {
      return iterator;
    }

    class FilteringIterator implements Iterator<T> {
      private final Iterator<T> delegate;
      @Nullable private T next;
      private boolean hasNext;

      FilteringIterator(Iterator<T> delegate) {
        this.delegate = delegate;
        advance();
      }

      private void advance() {
        while (delegate.hasNext()) {
          T candidate = delegate.next();
          if (!(candidate instanceof OpenTelemetryInstrument)) {
            next = candidate;
            hasNext = true;
            return;
          }
        }
        next = null;
        hasNext = false;
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public T next() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        T result = next;
        advance();
        return result;
      }
    }

    return new FilteringIterator(iterator);
  }

  private MicrometerSingletons() {}
}
