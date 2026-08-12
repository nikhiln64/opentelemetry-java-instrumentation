/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.micrometer.v1_5;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.incubator.config.DeclarativeConfigProperties;
import io.opentelemetry.instrumentation.api.incubator.config.internal.DeclarativeConfigUtil;
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistry;
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistryBuilder;
import io.opentelemetry.instrumentation.micrometer.v1_5.internal.Experimental;
import io.opentelemetry.instrumentation.micrometer.v1_5.internal.Internal;
import io.opentelemetry.instrumentation.micrometer.v1_5.internal.OpenTelemetryInstrument;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

public class MicrometerSingletons {

  private static final OpenTelemetryMeterRegistry meterRegistry;
  private static final IdentityHashMap<Object, RegistryContext> registryContexts =
      new IdentityHashMap<>();
  private static final ReentrantLock registryChangeLock = new ReentrantLock();

  static {
    DeclarativeConfigProperties config =
        DeclarativeConfigUtil.getInstrumentationConfig(GlobalOpenTelemetry.get(), "micrometer");
    OpenTelemetryMeterRegistryBuilder builder =
        OpenTelemetryMeterRegistry.builder(GlobalOpenTelemetry.get())
            .setPrometheusMode(config.get("prometheus_mode").getBoolean("enabled", false))
            .setBaseTimeUnit(TimeUnitParser.parseConfigValue(config.getString("base_time_unit")));
    Experimental.setMicrometerHistogramGaugesEnabled(
        builder, config.get("histogram_gauges").getBoolean("enabled", false));
    meterRegistry = (OpenTelemetryMeterRegistry) builder.build();
  }

  public static MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  public static void registerMeterRegistryContext(
      Object context, List<CompositeMeterRegistry> compositeMeterRegistries) {
    registryChangeLock.lock();
    try {
      synchronized (registryContexts) {
        registryContexts.put(context, new RegistryContext(compositeMeterRegistries));
        updateMetersHiddenFromSearch();
      }
    } finally {
      registryChangeLock.unlock();
    }
  }

  public static void unregisterMeterRegistryContext(Object context) {
    registryChangeLock.lock();
    try {
      synchronized (registryContexts) {
        registryContexts.remove(context);
        updateMetersHiddenFromSearch();
      }
    } finally {
      registryChangeLock.unlock();
    }
  }

  public static boolean beginMeterRegistryChange() {
    registryChangeLock.lock();
    return true;
  }

  public static void endMeterRegistryChange(
      CompositeMeterRegistry compositeMeterRegistry,
      MeterRegistry meterRegistry,
      boolean successful,
      boolean added) {
    try {
      if (successful) {
        synchronized (registryContexts) {
          for (RegistryContext context : registryContexts.values()) {
            if (added) {
              context.add(compositeMeterRegistry, meterRegistry);
            } else {
              context.remove(compositeMeterRegistry, meterRegistry);
            }
          }
          updateMetersHiddenFromSearch();
        }
      }
    } finally {
      registryChangeLock.unlock();
    }
  }

  private static void updateMetersHiddenFromSearch() {
    // The registry is JVM-global while Spring's registry topology is per application context.
    // Keep meters visible if any active context has no readable sibling; hiding them there would
    // make the metrics unavailable entirely.
    boolean metersHiddenFromSearch = !registryContexts.isEmpty();
    for (RegistryContext context : registryContexts.values()) {
      if (!context.hasReadableSibling()) {
        metersHiddenFromSearch = false;
        break;
      }
    }
    Internal.setMetersHiddenFromSearch(meterRegistry, metersHiddenFromSearch);
  }

  private static class RegistryContext {

    private final IdentityHashMap<CompositeMeterRegistry, IdentityHashMap<MeterRegistry, Boolean>>
        compositeMeterRegistries = new IdentityHashMap<>();

    RegistryContext(List<CompositeMeterRegistry> compositeMeterRegistries) {
      for (CompositeMeterRegistry compositeMeterRegistry : compositeMeterRegistries) {
        IdentityHashMap<MeterRegistry, Boolean> registries = new IdentityHashMap<>();
        for (MeterRegistry registry : compositeMeterRegistry.getRegistries()) {
          registries.put(registry, true);
        }
        if (registries.containsKey(meterRegistry)) {
          this.compositeMeterRegistries.put(compositeMeterRegistry, registries);
        }
      }
    }

    void add(CompositeMeterRegistry compositeMeterRegistry, MeterRegistry meterRegistry) {
      IdentityHashMap<MeterRegistry, Boolean> registries =
          compositeMeterRegistries.get(compositeMeterRegistry);
      if (registries != null) {
        registries.put(meterRegistry, true);
      }
    }

    void remove(CompositeMeterRegistry compositeMeterRegistry, MeterRegistry meterRegistry) {
      IdentityHashMap<MeterRegistry, Boolean> registries =
          compositeMeterRegistries.get(compositeMeterRegistry);
      if (registries != null) {
        registries.remove(meterRegistry);
      }
    }

    boolean hasReadableSibling() {
      if (compositeMeterRegistries.isEmpty()) {
        return false;
      }
      for (IdentityHashMap<MeterRegistry, Boolean> registries : compositeMeterRegistries.values()) {
        boolean hasReadableRegistry = false;
        for (MeterRegistry registry : registries.keySet()) {
          if (!(registry instanceof OpenTelemetryMeterRegistry)) {
            hasReadableRegistry = true;
            break;
          }
        }
        if (!hasReadableRegistry) {
          return false;
        }
      }
      return true;
    }
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
