package fr.agaetis.reactive.messaging.mqtt.server;

import static fr.agaetis.reactive.messaging.mqtt.server.TestUtils.createSubscriber;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.EXACTLY_ONCE;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.enterprise.inject.Any;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.junit5.auto.WeldJunit5AutoExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;

@ExtendWith({WeldJunit5AutoExtension.class, VertxExtension.class})
class MqttServerConnectorTest {

  @Test
  void test(@Any MqttServerConnector connector, VertxTestContext testContext) {
    final AtomicBoolean opened = new AtomicBoolean();
    final Map<String, String> configMap = new HashMap<>();
    configMap.put("port", "0");
    final List<TestMqttMessage> testMessages = new CopyOnWriteArrayList<>();
    testMessages.add(
        new TestMqttMessage("hello/topic", 1, "Hello world!", EXACTLY_ONCE.value(), false));
    testMessages.add(
        new TestMqttMessage("foo/bar", 2, "dkufhdspkjfosdjfs;", AT_LEAST_ONCE.value(),
            true));
    testMessages.add(
        new TestMqttMessage("foo/bar", -1, "Hello world!", AT_MOST_ONCE.value(), false));
    testMessages
        .add(new TestMqttMessage("sa/srt/tgvbc", 3, "Yeah", EXACTLY_ONCE.value(), true));
    final PublisherBuilder<MqttMessage> builder = (PublisherBuilder<MqttMessage>) connector
        .getPublisherBuilder(TestUtils.config(configMap));

    // The source is the same for every call
    assertEquals(builder, connector.getPublisherBuilder(TestUtils.config(configMap)));

    builder.buildRs().subscribe(createSubscriber(testContext, opened, testMessages));

    TestUtils.sendMqttMessages(testMessages, CompletableFuture.supplyAsync(() -> {
      await().until(opened::get);
      await().until(() -> connector.port() != 0);
      return connector.port();
    }), testContext);
  }

  @Test
  void testBroadcast(@Any MqttServerConnector connector, VertxTestContext testContext) {
    final AtomicBoolean opened = new AtomicBoolean();
    final Map<String, String> configMap = new HashMap<>();
    configMap.put("port", "0");
    configMap.put("broadcast", "true");
    final List<TestMqttMessage> testMessages = new CopyOnWriteArrayList<>();
    testMessages.add(
        new TestMqttMessage("hello/topic", 1, "Hello world!", EXACTLY_ONCE.value(), false));
    testMessages.add(
        new TestMqttMessage("foo/bar", 2, "dkufhdspkjfosdjfs;", AT_LEAST_ONCE.value(),
            true));
    testMessages.add(
        new TestMqttMessage("foo/bar", -1, "Hello world!", AT_MOST_ONCE.value(), false));
    testMessages
        .add(new TestMqttMessage("sa/srt/tgvbc", 3, "Yeah", EXACTLY_ONCE.value(), true));
    final PublisherBuilder<MqttMessage> builder = (PublisherBuilder<MqttMessage>) connector
        .getPublisherBuilder(TestUtils.config(configMap));

    // The source is the same for every call
    assertEquals(builder, connector.getPublisherBuilder(TestUtils.config(configMap)));

    final Publisher<MqttMessage> publisher = builder.buildRs();
    publisher.subscribe(createSubscriber(testContext, opened, testMessages));
    publisher.subscribe(createSubscriber(testContext, opened, testMessages));

    TestUtils.sendMqttMessages(testMessages, CompletableFuture.supplyAsync(() -> {
      await().until(opened::get);
      await().until(() -> connector.port() != 0);
      return connector.port();
    }), testContext);
  }
}
