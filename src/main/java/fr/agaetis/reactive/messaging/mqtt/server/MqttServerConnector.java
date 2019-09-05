package fr.agaetis.reactive.messaging.mqtt.server;

import io.vertx.reactivex.core.Vertx;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

@ApplicationScoped
@Connector(MqttServerConnector.CONNECTOR_NAME)
public class MqttServerConnector implements IncomingConnectorFactory {

  static final String CONNECTOR_NAME = "agaetis-mqtt-server";
  private final Vertx vertx;
  private final boolean internalVertxInstance;
  private MqttServerSource source = null;

  @Inject
  MqttServerConnector(Instance<Vertx> instanceOfVertx) {
    if (instanceOfVertx.isUnsatisfied()) {
      this.internalVertxInstance = true;
      this.vertx = Vertx.vertx();
    } else {
      this.internalVertxInstance = false;
      this.vertx = instanceOfVertx.get();
    }
  }

  public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
    if (internalVertxInstance) {
      vertx.close();
    }
  }

  @Override
  public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
    if (source == null) {
      source = new MqttServerSource(vertx, config);
    }
    return source.source();
  }

  @PreDestroy
  public synchronized void close() {
    if (source != null) {
      source.close();
    }
  }
}
