plugins {
  id("otel.javaagent-instrumentation")
}

muzzle {
  pass {
    group.set("org.apache.rocketmq")
    module.set("rocketmq-client-java")
    versions.set("[5.0.0,)")
    assertInverse.set(true)
  }
}

dependencies {
  library("org.apache.rocketmq:rocketmq-client-java:5.0.0")

  testInstrumentation(project(":instrumentation:rocketmq:rocketmq-client-4.8:javaagent"))

  // earlier versions have bugs that may make tests flaky.
  testLibrary("org.apache.rocketmq:rocketmq-client-java:5.0.2")
}

tasks {
  withType<Test>().configureEach {
    systemProperty("collectMetadata", otelProps.collectMetadata)
    usesService(gradle.sharedServices.registrations["testcontainersBuildService"].service)
  }

  val testReceiveSpanDisabled = register<Test>("testReceiveSpanDisabled") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      includeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    include("**/RocketMqClientSuppressReceiveSpanTest.*")
    jvmArgs("-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=false")
    systemProperty(
      "metadataConfig",
      "otel.instrumentation.messaging.experimental.receive-telemetry.enabled=false",
    )
  }

  val testMessagingPreview = register<Test>("testMessagingPreview") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      excludeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=true")
    jvmArgs("-Dotel.semconv-stability.preview=messaging")
    systemProperty("metadataConfig", "otel.semconv-stability.preview=messaging")
  }

  val testMessagingPreviewDefault = register<Test>("testMessagingPreviewDefault") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      includeTestsMatching("RocketMqClientTest.testEmptyPullReceive")
      includeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.semconv-stability.preview=messaging")
    systemProperty("metadataConfig", "otel.semconv-stability.preview=messaging")
  }

  val testMessagingPreviewReceiveDisabled = register<Test>("testMessagingPreviewReceiveDisabled") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      includeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=false")
    jvmArgs("-Dotel.semconv-stability.preview=messaging")
    systemProperty("metadataConfig", "otel.semconv-stability.preview=messaging")
  }

  val testV3Preview = register<Test>("testV3Preview") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      excludeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=true")
    jvmArgs("-Dotel.instrumentation.common.v3-preview=true")
    systemProperty("metadataConfig", "otel.instrumentation.common.v3-preview=true")
  }

  val testV3PreviewDefault = register<Test>("testV3PreviewDefault") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      includeTestsMatching("RocketMqClientTest.testEmptyPullReceive")
      includeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.instrumentation.common.v3-preview=true")
    systemProperty("metadataConfig", "otel.instrumentation.common.v3-preview=true")
  }

  val testV3PreviewDisabled = register<Test>("testV3PreviewDisabled") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      includeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.instrumentation.common.v3-preview=false")
    systemProperty("metadataConfig", "otel.instrumentation.common.v3-preview=false")
  }

  val testBothSemconv = register<Test>("testBothSemconv") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
      includeTestsMatching("RocketMqClientTest.testSendAndConsumeNormalMessage")
      includeTestsMatching("RocketMqClientTest.testConsumeFailure")
    }
    jvmArgs("-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=true")
    jvmArgs("-Dotel.semconv-stability.preview=messaging/dup")
    systemProperty("metadataConfig", "otel.semconv-stability.preview=messaging/dup")
  }

  test {
    filter {
      excludeTestsMatching("RocketMqClientSuppressReceiveSpanTest")
    }
    jvmArgs("-Dotel.instrumentation.messaging.experimental.receive-telemetry.enabled=true")
    systemProperty(
      "metadataConfig",
      "otel.instrumentation.messaging.experimental.receive-telemetry.enabled=true",
    )
  }

  check {
    dependsOn(
      testReceiveSpanDisabled,
      testMessagingPreview,
      testMessagingPreviewDefault,
      testMessagingPreviewReceiveDisabled,
      testV3Preview,
      testV3PreviewDefault,
      testV3PreviewDisabled,
      testBothSemconv,
    )
  }

  if (otelProps.denyUnsafe) {
    withType<Test>().configureEach {
      enabled = false
    }
  }
}
