/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v1_1;

import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.implementsInterface;
import static io.opentelemetry.javaagent.instrumentation.jms.v1_1.JmsSingletons.consumerProcessAfterReceiveInstrumenter;
import static io.opentelemetry.javaagent.instrumentation.jms.v1_1.JmsSingletons.consumerProcessInstrumenter;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.javaagent.bootstrap.jms.JmsReceiveContextHolder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import io.opentelemetry.javaagent.instrumentation.jms.common.v1_1.MessageWithDestination;
import javax.annotation.Nullable;
import javax.jms.Message;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

class JmsMessageListenerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<ClassLoader> classLoaderOptimization() {
    return hasClassesNamed("javax.jms.MessageListener");
  }

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return implementsInterface(named("javax.jms.MessageListener"));
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("onMessage").and(takesArgument(0, named("javax.jms.Message"))).and(isPublic()),
        getClass().getName() + "$MessageListenerAdvice");
  }

  @SuppressWarnings("unused")
  public static class MessageListenerAdvice {

    public static class AdviceScope {
      private final MessageWithDestination messageWithDestination;
      private final Instrumenter<MessageWithDestination, Void> instrumenter;
      private final Context context;
      private final Scope scope;

      private AdviceScope(
          MessageWithDestination messageWithDestination,
          Instrumenter<MessageWithDestination, Void> instrumenter,
          Context context,
          Scope scope) {
        this.messageWithDestination = messageWithDestination;
        this.instrumenter = instrumenter;
        this.context = context;
        this.scope = scope;
      }

      @Nullable
      public static AdviceScope start(Message message) {
        Context currentContext = Context.current();
        boolean stableMessagingSemconv = emitStableMessagingSemconv();
        Context parentContext = currentContext;
        if (!stableMessagingSemconv) {
          Context receiveContext = JmsReceiveContextHolder.getReceiveContext(currentContext);
          if (receiveContext != null) {
            parentContext = receiveContext;
          }
        }
        MessageWithDestination messageWithDestination =
            MessageWithDestination.create(JavaxMessageAdapter.create(message), null);
        boolean receiveTelemetryRecorded =
            JmsReceiveContextHolder.isReceiveTelemetryRecorded(currentContext);
        Instrumenter<MessageWithDestination, Void> instrumenter =
            receiveTelemetryRecorded
                ? consumerProcessAfterReceiveInstrumenter()
                : consumerProcessInstrumenter();

        if (!instrumenter.shouldStart(parentContext, messageWithDestination)) {
          return null;
        }

        Context context;
        if (!stableMessagingSemconv && !receiveTelemetryRecorded) {
          // The legacy consumer instrumenter extracts remote context and checks for context leaks.
          try (Scope ignored = Context.root().makeCurrent()) {
            context = instrumenter.start(parentContext, messageWithDestination);
          }
        } else {
          context = instrumenter.start(parentContext, messageWithDestination);
        }
        return new AdviceScope(
            messageWithDestination, instrumenter, context, context.makeCurrent());
      }

      public void end(@Nullable Throwable throwable) {
        scope.close();
        instrumenter.end(context, messageWithDestination, null, throwable);
      }
    }

    @Nullable
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static AdviceScope onEnter(@Advice.Argument(0) Message message) {
      return AdviceScope.start(message);
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class, inline = false)
    public static void stopSpan(
        @Advice.Thrown @Nullable Throwable throwable,
        @Advice.Enter @Nullable AdviceScope adviceScope) {
      if (adviceScope != null) {
        adviceScope.end(throwable);
      }
    }
  }
}
