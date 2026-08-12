/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.spring.boot.actuator.autoconfigure.v2_0;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.instrumentation.testing.internal.AutoCleanupExtension;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import io.opentelemetry.javaagent.instrumentation.micrometer.v1_5.MicrometerSingletons;
import io.opentelemetry.javaagent.instrumentation.spring.boot.actuator.autoconfigure.v2_0.SpringApp.TestBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

class ActuatorTest {

  @RegisterExtension
  static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @RegisterExtension static final AutoCleanupExtension cleanup = AutoCleanupExtension.create();

  @Test
  void shouldInjectOtelMeterRegistry() throws ReflectiveOperationException {
    SpringApplication app = new SpringApplication(SpringApp.class);
    ConfigurableApplicationContext context = app.run();
    cleanup.deferCleanup(context);

    TestBean testBean = context.getBean(TestBean.class);
    testBean.inc();

    testing.waitAndAssertMetrics(
        "io.opentelemetry.micrometer-1.5",
        metric ->
            metric
                .hasName("test-counter")
                .hasUnit("thingies")
                .hasDoubleSumSatisfying(
                    sum ->
                        sum.isMonotonic()
                            .hasPointsSatisfying(
                                point ->
                                    point
                                        .hasValue(1)
                                        .hasAttributesSatisfyingExactly(
                                            equalTo(stringKey("tag"), "value")))));

    MeterRegistry meterRegistry = context.getBean(MeterRegistry.class);
    assertThat(meterRegistry).isInstanceOf(CompositeMeterRegistry.class);
    CompositeMeterRegistry composite = (CompositeMeterRegistry) meterRegistry;
    MeterRegistry otelMeterRegistry = MicrometerSingletons.meterRegistry();

    Set<MeterRegistry> registries = composite.getRegistries();
    assertThat(registries).contains(otelMeterRegistry);
    assertThat(registries).filteredOn(SimpleMeterRegistry.class::isInstance).hasSize(1);
    SimpleMeterRegistry fallbackRegistry =
        registries.stream()
            .filter(SimpleMeterRegistry.class::isInstance)
            .map(SimpleMeterRegistry.class::cast)
            .findFirst()
            .get();
    assertThat(fallbackRegistry.find("test-counter").counter()).isNotNull();
    assertThat(otelMeterRegistry.find("test-counter").counter()).isNull();

    assertThat(metricValue(context, "test-counter")).isEqualTo(1);

    composite.remove(fallbackRegistry);
    assertThat(otelMeterRegistry.find("test-counter").counter()).isNotNull();

    SimpleMeterRegistry addedAfterStartup = new SimpleMeterRegistry();
    composite.add(addedAfterStartup);
    assertThat(composite.getRegistries()).contains(addedAfterStartup);
    assertThat(addedAfterStartup.find("test-counter").counter()).isNotNull();
    assertThat(otelMeterRegistry.find("test-counter").counter()).isNull();
    composite.remove(addedAfterStartup);
  }

  @Test
  void shouldKeepMetersVisibleWithoutAnotherRegistry() {
    ConfigurableApplicationContext context = runOtelOnlyApplication();
    cleanup.deferCleanup(context);

    context.getBean(TestBean.class).inc();

    MeterRegistry otelMeterRegistry = MicrometerSingletons.meterRegistry();
    assertThat(otelMeterRegistry.find("test-counter").counter()).isNotNull();

    ConfigurableApplicationContext contextWithReadableRegistry =
        new SpringApplication(SpringApp.class).run();
    cleanup.deferCleanup(contextWithReadableRegistry);

    // The registry is shared by both contexts. Keeping meters visible is safer than hiding the only
    // copy available to a context that has no readable sibling.
    assertThat(otelMeterRegistry.find("test-counter").counter()).isNotNull();
  }

  @Test
  void shouldKeepMetersVisibleWithOnlyOpenTelemetryRegistries()
      throws ReflectiveOperationException {
    ConfigurableApplicationContext context = new SpringApplication(SpringApp.class).run();
    cleanup.deferCleanup(context);

    context.getBean(TestBean.class).inc();

    MeterRegistry otelMeterRegistry = MicrometerSingletons.meterRegistry();
    CompositeMeterRegistry composite =
        (CompositeMeterRegistry) context.getBean(MeterRegistry.class);
    removeRegistriesOtherThan(composite, otelMeterRegistry);
    MeterRegistry userDefinedOtelRegistry = newOpenTelemetryMeterRegistry(otelMeterRegistry);
    cleanup.deferCleanup(userDefinedOtelRegistry::close);
    composite.add(userDefinedOtelRegistry);

    assertThat(otelMeterRegistry.find("test-counter").counter()).isNotNull();
  }

  @Test
  void shouldTrackRegistryAddedBeforeRebindingFailure() {
    ConfigurableApplicationContext context = new SpringApplication(SpringApp.class).run();
    cleanup.deferCleanup(context);

    MeterRegistry otelMeterRegistry = MicrometerSingletons.meterRegistry();
    CompositeMeterRegistry composite =
        (CompositeMeterRegistry) context.getBean(MeterRegistry.class);
    removeRegistriesOtherThan(composite, otelMeterRegistry);
    SimpleMeterRegistry throwingRegistry =
        new SimpleMeterRegistry() {
          @Override
          protected Counter newCounter(Meter.Id id) {
            throw new IllegalStateException("test");
          }
        };
    cleanup.deferCleanup(throwingRegistry::close);

    assertThatThrownBy(() -> composite.add(throwingRegistry))
        .isInstanceOf(IllegalStateException.class);

    assertThat(composite.getRegistries()).contains(throwingRegistry);
    assertThat(otelMeterRegistry.find("test-counter").counter()).isNull();
  }

  private static void removeRegistriesOtherThan(
      CompositeMeterRegistry composite, MeterRegistry retainedRegistry) {
    for (MeterRegistry registry : new ArrayList<>(composite.getRegistries())) {
      if (registry != retainedRegistry) {
        composite.remove(registry);
      }
    }
  }

  private static MeterRegistry newOpenTelemetryMeterRegistry(MeterRegistry otelMeterRegistry)
      throws ReflectiveOperationException {
    Method createMethod =
        Arrays.stream(otelMeterRegistry.getClass().getMethods())
            .filter(method -> method.getName().equals("create"))
            .findFirst()
            .get();
    Class<?> openTelemetryClass = createMethod.getParameterTypes()[0];
    Class<?> globalOpenTelemetryClass =
        Class.forName(
            openTelemetryClass.getPackage().getName() + ".GlobalOpenTelemetry",
            true,
            openTelemetryClass.getClassLoader());
    Object openTelemetry = globalOpenTelemetryClass.getMethod("get").invoke(null);
    return (MeterRegistry) createMethod.invoke(null, openTelemetry);
  }

  private static ConfigurableApplicationContext runOtelOnlyApplication() {
    SpringApplication app = new SpringApplication(SpringApp.class);
    Map<String, Object> properties = new HashMap<>();
    properties.put("management.metrics.export.prometheus.enabled", false);
    properties.put("management.metrics.export.simple.enabled", false);
    properties.put("management.prometheus.metrics.export.enabled", false);
    properties.put("management.simple.metrics.export.enabled", false);
    app.setDefaultProperties(properties);
    return app.run();
  }

  private static double metricValue(ConfigurableApplicationContext context, String meterName)
      throws ReflectiveOperationException {
    ClassLoader classLoader = context.getClassLoader();
    Class<?> endpointClass;
    try {
      endpointClass =
          Class.forName(
              "org.springframework.boot.actuate.metrics.MetricsEndpoint", false, classLoader);
    } catch (ClassNotFoundException ignored) {
      endpointClass =
          Class.forName(
              "org.springframework.boot.micrometer.metrics.actuate.endpoint.MetricsEndpoint",
              false,
              classLoader);
    }

    Object endpoint;
    if (context.getBeansOfType(endpointClass).isEmpty()) {
      endpoint =
          endpointClass
              .getConstructor(MeterRegistry.class)
              .newInstance(context.getBean(MeterRegistry.class));
    } else {
      endpoint = context.getBeansOfType(endpointClass).values().iterator().next();
    }
    Method metric = endpointClass.getMethod("metric", String.class, List.class);
    Object response = metric.invoke(endpoint, meterName, null);
    Method getMeasurements = response.getClass().getMethod("getMeasurements");
    List<?> measurements = (List<?>) getMeasurements.invoke(response);
    assertThat(measurements).hasSize(1);
    Object sample = measurements.get(0);
    return ((Number) sample.getClass().getMethod("getValue").invoke(sample)).doubleValue();
  }
}
