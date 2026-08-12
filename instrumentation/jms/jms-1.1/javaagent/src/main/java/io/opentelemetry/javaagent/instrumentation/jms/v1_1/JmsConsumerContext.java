/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v1_1;

import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

public final class JmsConsumerContext {

  private static final VirtualField<MessageConsumer, String> consumerSubscriptionNameField =
      VirtualField.find(MessageConsumer.class, String.class);
  private static final VirtualField<Message, String> messageSubscriptionNameField =
      VirtualField.find(Message.class, String.class);
  private static final VirtualField<MessageConsumer, ConsumerListenerRegistration>
      consumerListenerRegistrationField =
          VirtualField.find(MessageConsumer.class, ConsumerListenerRegistration.class);
  private static final VirtualField<MessageListener, ListenerSubscriptions>
      listenerSubscriptionsField =
          VirtualField.find(MessageListener.class, ListenerSubscriptions.class);
  private static final VirtualField<Session, ConsumerRegistry> sessionConsumersField =
      VirtualField.find(Session.class, ConsumerRegistry.class);
  private static final VirtualField<Connection, SessionRegistry> connectionSessionsField =
      VirtualField.find(Connection.class, SessionRegistry.class);

  public static void setSubscriptionName(MessageConsumer consumer, String subscriptionName) {
    consumerSubscriptionNameField.set(consumer, subscriptionName);
  }

  public static void setSubscriptionName(Message message, @Nullable String subscriptionName) {
    messageSubscriptionNameField.set(message, subscriptionName);
  }

  @Nullable
  public static String getSubscriptionName(MessageConsumer consumer) {
    return consumerSubscriptionNameField.get(consumer);
  }

  @Nullable
  public static String getSubscriptionName(Message message) {
    return messageSubscriptionNameField.get(message);
  }

  @Nullable
  public static String getSubscriptionName(MessageListener messageListener) {
    ListenerSubscriptions subscriptions = listenerSubscriptionsField.get(messageListener);
    return subscriptions == null ? null : subscriptions.getSubscriptionName();
  }

  public static void registerConsumer(Session session, MessageConsumer consumer) {
    getOrCreateConsumerRegistry(session).add(consumer);
  }

  public static void registerSession(Connection connection, Session session) {
    getOrCreateSessionRegistry(connection).add(session);
  }

  public static void closeConnection(Connection connection) {
    SessionRegistry sessions = connectionSessionsField.get(connection);
    if (sessions != null) {
      sessions.close();
    }
  }

  public static void closeSession(Session session) {
    ConsumerRegistry consumers = sessionConsumersField.get(session);
    if (consumers != null) {
      consumers.close();
    }
  }

  @Nullable
  public static ListenerUpdate updateMessageListener(
      MessageConsumer consumer, @Nullable MessageListener messageListener) {
    ConsumerListenerRegistration registration = getOrCreateConsumerListenerRegistration(consumer);
    synchronized (registration) {
      if (registration.closed || registration.messageListener == messageListener) {
        return null;
      }
      MessageListener previousMessageListener = registration.messageListener;
      replaceMessageListener(registration, consumer, messageListener);
      return new ListenerUpdate(registration, previousMessageListener, registration.version);
    }
  }

  public static void rollbackMessageListener(
      MessageConsumer consumer, @Nullable ListenerUpdate update) {
    if (update == null) {
      return;
    }
    ConsumerListenerRegistration registration = consumerListenerRegistrationField.get(consumer);
    if (registration != update.registration) {
      return;
    }
    synchronized (registration) {
      if (!registration.closed && registration.version == update.version) {
        replaceMessageListener(registration, consumer, update.previousMessageListener);
      }
    }
  }

  public static void closeConsumer(MessageConsumer consumer) {
    ConsumerListenerRegistration registration = consumerListenerRegistrationField.get(consumer);
    if (registration == null) {
      return;
    }
    synchronized (registration) {
      if (!registration.closed) {
        replaceMessageListener(registration, consumer, null);
        registration.closed = true;
      }
    }
  }

  private static void replaceMessageListener(
      ConsumerListenerRegistration registration,
      MessageConsumer consumer,
      @Nullable MessageListener messageListener) {
    if (registration.messageListener != null) {
      removeSubscription(registration.messageListener, consumer);
    }
    registration.messageListener = messageListener;
    registration.version++;
    if (messageListener != null) {
      addSubscription(messageListener, consumer, consumerSubscriptionNameField.get(consumer));
    }
  }

  private static ConsumerListenerRegistration getOrCreateConsumerListenerRegistration(
      MessageConsumer consumer) {
    ConsumerListenerRegistration registration = consumerListenerRegistrationField.get(consumer);
    if (registration == null) {
      synchronized (consumerListenerRegistrationField) {
        registration = consumerListenerRegistrationField.get(consumer);
        if (registration == null) {
          registration = new ConsumerListenerRegistration();
          consumerListenerRegistrationField.set(consumer, registration);
        }
      }
    }
    return registration;
  }

  private static ConsumerRegistry getOrCreateConsumerRegistry(Session session) {
    ConsumerRegistry registry = sessionConsumersField.get(session);
    if (registry == null) {
      synchronized (sessionConsumersField) {
        registry = sessionConsumersField.get(session);
        if (registry == null) {
          registry = new ConsumerRegistry();
          sessionConsumersField.set(session, registry);
        }
      }
    }
    return registry;
  }

  private static SessionRegistry getOrCreateSessionRegistry(Connection connection) {
    SessionRegistry registry = connectionSessionsField.get(connection);
    if (registry == null) {
      synchronized (connectionSessionsField) {
        registry = connectionSessionsField.get(connection);
        if (registry == null) {
          registry = new SessionRegistry();
          connectionSessionsField.set(connection, registry);
        }
      }
    }
    return registry;
  }

  private static void addSubscription(
      MessageListener messageListener,
      MessageConsumer consumer,
      @Nullable String subscriptionName) {
    ListenerSubscriptions subscriptions = listenerSubscriptionsField.get(messageListener);
    if (subscriptions == null) {
      synchronized (listenerSubscriptionsField) {
        subscriptions = listenerSubscriptionsField.get(messageListener);
        if (subscriptions == null) {
          subscriptions = new ListenerSubscriptions();
          listenerSubscriptionsField.set(messageListener, subscriptions);
        }
      }
    }
    subscriptions.add(consumer, subscriptionName);
  }

  private static void removeSubscription(
      MessageListener messageListener, MessageConsumer consumer) {
    ListenerSubscriptions subscriptions = listenerSubscriptionsField.get(messageListener);
    if (subscriptions != null) {
      subscriptions.remove(consumer);
    }
  }

  private JmsConsumerContext() {}

  public static class ListenerUpdate {
    private final ConsumerListenerRegistration registration;
    private final MessageListener previousMessageListener;
    private final long version;

    private ListenerUpdate(
        ConsumerListenerRegistration registration,
        @Nullable MessageListener previousMessageListener,
        long version) {
      this.registration = registration;
      this.previousMessageListener = previousMessageListener;
      this.version = version;
    }
  }

  private static class ConsumerListenerRegistration {
    @Nullable private MessageListener messageListener;
    private long version;
    private boolean closed;
  }

  private static class ListenerSubscriptions {
    private final List<ListenerSubscription> subscriptions = new ArrayList<>();

    synchronized void add(MessageConsumer consumer, @Nullable String subscriptionName) {
      Iterator<ListenerSubscription> iterator = subscriptions.iterator();
      while (iterator.hasNext()) {
        ListenerSubscription subscription = iterator.next();
        MessageConsumer registeredConsumer = subscription.consumer.get();
        if (registeredConsumer == null) {
          iterator.remove();
        } else if (registeredConsumer == consumer) {
          subscription.subscriptionName = subscriptionName;
          return;
        }
      }
      subscriptions.add(new ListenerSubscription(consumer, subscriptionName));
    }

    synchronized void remove(MessageConsumer consumer) {
      Iterator<ListenerSubscription> iterator = subscriptions.iterator();
      while (iterator.hasNext()) {
        MessageConsumer registeredConsumer = iterator.next().consumer.get();
        if (registeredConsumer == null || registeredConsumer == consumer) {
          iterator.remove();
        }
      }
    }

    @Nullable
    synchronized String getSubscriptionName() {
      boolean found = false;
      String subscriptionName = null;
      Iterator<ListenerSubscription> iterator = subscriptions.iterator();
      while (iterator.hasNext()) {
        ListenerSubscription subscription = iterator.next();
        if (subscription.consumer.get() == null) {
          iterator.remove();
          continue;
        }
        String candidate = subscription.subscriptionName;
        if (found && !Objects.equals(subscriptionName, candidate)) {
          return null;
        }
        found = true;
        subscriptionName = candidate;
      }
      return subscriptionName;
    }
  }

  private static class ListenerSubscription {
    private final WeakReference<MessageConsumer> consumer;
    @Nullable private String subscriptionName;

    private ListenerSubscription(MessageConsumer consumer, @Nullable String subscriptionName) {
      this.consumer = new WeakReference<>(consumer);
      this.subscriptionName = subscriptionName;
    }
  }

  private static class ConsumerRegistry {
    private final WeakIdentitySet<MessageConsumer> consumers = new WeakIdentitySet<>();
    private boolean closed;

    synchronized void add(MessageConsumer consumer) {
      if (closed) {
        closeConsumer(consumer);
      } else {
        consumers.add(consumer);
      }
    }

    synchronized void close() {
      closed = true;
      for (MessageConsumer consumer : consumers.drain()) {
        closeConsumer(consumer);
      }
    }
  }

  private static class SessionRegistry {
    private final WeakIdentitySet<Session> sessions = new WeakIdentitySet<>();
    private boolean closed;

    synchronized void add(Session session) {
      if (closed) {
        closeSession(session);
      } else {
        sessions.add(session);
      }
    }

    synchronized void close() {
      closed = true;
      for (Session session : sessions.drain()) {
        closeSession(session);
      }
    }
  }

  private static class WeakIdentitySet<T> {
    private final ReferenceQueue<T> referenceQueue = new ReferenceQueue<>();
    private final Set<IdentityWeakReference<T>> values = new HashSet<>();

    void add(T value) {
      expungeStaleValues();
      values.add(new IdentityWeakReference<>(value, referenceQueue));
    }

    List<T> drain() {
      List<T> liveValues = new ArrayList<>();
      for (IdentityWeakReference<T> reference : values) {
        T value = reference.get();
        if (value != null) {
          liveValues.add(value);
        }
      }
      values.clear();
      return liveValues;
    }

    private void expungeStaleValues() {
      Reference<? extends T> reference;
      while ((reference = referenceQueue.poll()) != null) {
        values.remove(reference);
      }
    }
  }

  private static class IdentityWeakReference<T> extends WeakReference<T> {
    private final int hashCode;

    private IdentityWeakReference(T value, ReferenceQueue<T> referenceQueue) {
      super(value, referenceQueue);
      hashCode = System.identityHashCode(value);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof IdentityWeakReference)) {
        return false;
      }
      Object value = get();
      return value != null && value == ((IdentityWeakReference<?>) other).get();
    }
  }
}
