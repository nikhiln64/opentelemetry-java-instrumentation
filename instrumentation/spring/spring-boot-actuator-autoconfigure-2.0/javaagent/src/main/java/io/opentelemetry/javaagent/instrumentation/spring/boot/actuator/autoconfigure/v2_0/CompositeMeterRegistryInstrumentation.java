/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.spring.boot.actuator.autoconfigure.v2_0;

import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import io.opentelemetry.javaagent.instrumentation.micrometer.v1_5.MicrometerSingletons;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

class CompositeMeterRegistryInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("io.micrometer.core.instrument.composite.CompositeMeterRegistry");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isMethod().and(named("add")).and(takesArguments(1)), getClass().getName() + "$AddAdvice");
    transformer.applyAdviceToMethod(
        isMethod().and(named("remove")).and(takesArguments(1)),
        getClass().getName() + "$RemoveAdvice");
  }

  @SuppressWarnings("unused")
  public static class AddAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static boolean onEnter() {
      return MicrometerSingletons.beginMeterRegistryChange();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(
        @Advice.This CompositeMeterRegistry compositeMeterRegistry,
        @Advice.Argument(0) MeterRegistry meterRegistry,
        @Advice.Enter boolean locked,
        @Advice.Thrown Throwable throwable) {
      if (locked) {
        MicrometerSingletons.endMeterRegistryChange(
            compositeMeterRegistry, meterRegistry, throwable == null, true);
      }
    }
  }

  @SuppressWarnings("unused")
  public static class RemoveAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static boolean onEnter() {
      return MicrometerSingletons.beginMeterRegistryChange();
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(
        @Advice.This CompositeMeterRegistry compositeMeterRegistry,
        @Advice.Argument(0) MeterRegistry meterRegistry,
        @Advice.Enter boolean locked,
        @Advice.Thrown Throwable throwable) {
      if (locked) {
        MicrometerSingletons.endMeterRegistryChange(
            compositeMeterRegistry, meterRegistry, throwable == null, false);
      }
    }
  }
}
