package fr.agaetis.reactive.messaging.mqtt.server;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class MqttServerSourceTest {

  Config config(Map<String, String> map) {
    return new SmallRyeConfigBuilder()
        .withSources(new PropertiesConfigSource(map, "", 0)).build();
  }

  @Test
  void simpleTest(Vertx vertx, VertxTestContext testContext) throws Throwable {
    final Map<String, String> configMap = new HashMap<>();
    configMap.put("port", "0");
    final MqttServerSource source = new MqttServerSource(vertx, config(configMap));
    final PublisherBuilder<MqttMessage> mqttMessagePublisherBuilder = source.source();
    final Checkpoint clientConnected = testContext.checkpoint();
    final Checkpoint messageSent = testContext.checkpoint();
    final Checkpoint messageReceived = testContext.checkpoint();
    final Checkpoint messageAcknowledged = testContext.checkpoint();
    final Checkpoint clientClosed = testContext.checkpoint();

    mqttMessagePublisherBuilder.forEach(mqttMessage -> {
      testContext.verify(() -> {
        messageReceived.flag();
        assertEquals("Hello world!", new String(mqttMessage.getPayload()));
        assertEquals(1, mqttMessage.getMessageId());
        assertEquals(MqttQoS.EXACTLY_ONCE, mqttMessage.getQosLevel());
        assertEquals("hello/topic", mqttMessage.getTopic());
        assertFalse(mqttMessage.isRetain());
        assertFalse(mqttMessage.isDuplicate());
      });
      mqttMessage.ack().thenApply(aVoid -> {
        messageAcknowledged.flag();
        return aVoid;
      });
    }).run();
    new Thread(() -> {
      try {
        // Wait for the server to listen to a random port
        await().until(source::port, port -> port != 0);
        MqttClient mqttClient = new MqttClient("tcp://localhost:" + source.port(),
            MqttClient.generateClientId());
        mqttClient.connect();
        clientConnected.flag();
        mqttClient.publish("hello/topic", "Hello world!".getBytes(), 2, false);
        messageSent.flag();
        mqttClient.disconnect();
        mqttClient.close();
        clientClosed.flag();
      } catch (MqttException e) {
        testContext.failNow(e);
      }
    }).start();
    assertTrue(testContext.awaitCompletion(5, TimeUnit.MINUTES));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }
}
