package fr.agaetis.reactive.messaging.mqtt.server;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.reactivex.processors.BehaviorProcessor;
import io.vertx.core.net.NetServerOptions;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.mqtt.MqttServer;
import java.util.concurrent.CompletableFuture;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MqttServerSource {

  private final Logger logger = LoggerFactory.getLogger(MqttServerSource.class);
  private final MqttServer mqttServer;
  private final PublisherBuilder<MqttMessage> source;

  private static MqttServerOptions mqttServerOptions(Config config) {
    final MqttServerOptions options = new MqttServerOptions();
    options.setAutoClientId(config.getOptionalValue("auto-client-id", Boolean.class).orElse(true));
    options.setSsl(
        config.getOptionalValue("ssl", Boolean.class).orElse(MqttServerOptions.DEFAULT_SSL));
    // TODO set KeyCertOptions if SSL, c.f. https://vertx.io/docs/vertx-mqtt/java/#_handling_client_connection_disconnection_with_ssl_tls_support
    options.setMaxMessageSize(config.getOptionalValue("max-message-size", Integer.class)
        .orElse(MqttServerOptions.DEFAULT_MAX_MESSAGE_SIZE));
    options.setTimeoutOnConnect(config.getOptionalValue("timeout-on-connect", Integer.class)
        .orElse(MqttServerOptions.DEFAULT_TIMEOUT_ON_CONNECT));
    options.setReceiveBufferSize(config.getOptionalValue("receive-buffer-size", Integer.class)
        .orElse(MqttServerOptions.DEFAULT_RECEIVE_BUFFER_SIZE));
    final int defaultPort =
        options.isSsl() ? MqttServerOptions.DEFAULT_TLS_PORT : MqttServerOptions.DEFAULT_PORT;
    options.setPort(config.getOptionalValue("port", Integer.class).orElse(defaultPort));
    options.setHost(
        config.getOptionalValue("host", String.class).orElse(NetServerOptions.DEFAULT_HOST));
    return options;
  }

  MqttServerSource(Vertx vertx, Config config) {
    this(vertx, mqttServerOptions(config));
  }

  private MqttServerSource(Vertx vertx, MqttServerOptions options) {
    this.mqttServer = MqttServer.create(vertx, options);
    final BehaviorProcessor<MqttMessage> processor = BehaviorProcessor.create();

    mqttServer.exceptionHandler(error -> {
      logger.error("Exception thrown", error);
      processor.onError(error);
    });

    mqttServer.endpointHandler(endpoint -> {
      logger.debug("MQTT client [{}] request to connect, clean session = {}",
          endpoint.clientIdentifier(), endpoint.isCleanSession());

      if (endpoint.auth() != null) {
        logger.trace("[username = {}, password = {}]", endpoint.auth().getUsername(),
            endpoint.auth().getPassword());
      }
      if (endpoint.will() != null) {
        logger.trace("[will topic = {} msg = {} QoS = {} isRetain = {}]",
            endpoint.will().getWillTopic(), endpoint.will().getWillMessageBytes(),
            endpoint.will().getWillQos(), endpoint.will().isWillRetain());
      }

      logger.trace("[keep alive timeout = {}]", endpoint.keepAliveTimeSeconds());

      endpoint.exceptionHandler(
          error -> logger.error("Error with client " + endpoint.clientIdentifier(), error));

      endpoint.disconnectHandler(
          v -> logger.debug("MQTT client [{}] disconnected", endpoint.clientIdentifier()));

      endpoint.pingHandler(
          v -> logger.trace("Ping received from client [{}]", endpoint.clientIdentifier()));

      endpoint.publishHandler(message -> {
        logger.debug("Just received message [{}] with QoS [{}] from client [{}]",
            message.payload(),
            message.qosLevel(), endpoint.clientIdentifier());

        processor.onNext(new MqttMessage(message, endpoint.clientIdentifier(), () -> {
          if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            logger.trace("Send PUBACK to client [{}] for message [{}]",
                endpoint.clientIdentifier(),
                message.messageId());
            endpoint.publishAcknowledge(message.messageId());
          } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
            logger.trace("Send PUBREC to client [{}] for message [{}]",
                endpoint.clientIdentifier(),
                message.messageId());
            endpoint.publishReceived(message.messageId());
          }
          return CompletableFuture.completedFuture(null);
        }));
      });

      endpoint.publishReleaseHandler(messageId -> {
        logger.trace("Send PUBCOMP to client [{}] for message [{}]", endpoint.clientIdentifier(),
            messageId);
        endpoint.publishComplete(messageId);
      });

      endpoint.subscribeHandler(subscribeMessage -> {
        logger.trace("Received subscription message {} from client [{}], closing connection",
            subscribeMessage, endpoint.clientIdentifier());
        endpoint.close();
      });

      // accept connection from the remote client
      // this implementation doesn't keep track of sessions
      endpoint.accept(false);
    });

    this.source = ReactiveStreams.fromPublisher(processor
        .delaySubscription(mqttServer.rxListen()
            .doOnSuccess(ignored -> logger
                .info("MQTT server listening on {}:{}", options.getHost(), mqttServer.actualPort()))
            .doOnError(throwable -> logger.error("Failed to start MQTT server", throwable))
            .toFlowable())
        .doOnSubscribe(subscription -> logger.debug("New subscriber added {}", subscription)));
  }

  synchronized PublisherBuilder<MqttMessage> source() {
    return source;
  }

  synchronized void close() {
    mqttServer.close(ar -> {
      if (ar.failed()) {
        logger.warn("An exception has been caught while closing the MQTT server", ar.cause());
      } else {
        logger.debug("MQTT server closed");
      }
    });
  }

  synchronized int port() {
    return mqttServer.actualPort();
  }
}
