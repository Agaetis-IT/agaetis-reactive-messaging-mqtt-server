package fr.agaetis.reactive.messaging.mqtt.server;

import io.vertx.junit5.VertxExtension;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.jboss.weld.junit5.auto.WeldJunit5AutoExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

@ExtendWith({WeldJunit5AutoExtension.class, VertxExtension.class})
public class MqttServerConnectorTest {

  @Test
  void test(@Any MqttServerConnector connector, @Default Config config) {
    List<Message> messages = new ArrayList<>();
    AtomicBoolean opened = new AtomicBoolean();
    PublisherBuilder<? extends Message> builder = connector.getPublisherBuilder(config);
  }

  private <T> Subscriber<T> createSubscriber(List<T> messages, AtomicBoolean opened) {
    return new Subscriber<T>() {
      Subscription sub;

      @Override
      public void onSubscribe(Subscription s) {
        this.sub = s;
        sub.request(5);
        opened.set(true);
      }

      @Override
      public void onNext(T message) {
        messages.add(message);
        sub.request(1);
      }

      @Override
      public void onError(Throwable t) {
        LoggerFactory.getLogger("SUBSCRIBER").error("Error caught in stream", t);
      }

      @Override
      public void onComplete() {
        // Do nothing.
      }
    };
  }
}
