![GitHub](https://img.shields.io/github/license/Agaetis-IT/agaetis-reactive-messaging-mqtt-server)
[![Maintainability](https://api.codeclimate.com/v1/badges/9bd7efe823b83b376b31/maintainability)](https://codeclimate.com/github/Agaetis-IT/agaetis-reactive-messaging-mqtt-server/maintainability)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v1.4%20adopted-ff69b4.svg)](code-of-conduct.md)

# MQTT server source connector for MicroProfile reactive messaging

An implementation of a Source connector complying with the (next to be) [Eclipse MicroProfile 
Reactive Messaging](https://github.com/eclipse/microprofile-reactive-messaging) specification for source connector.

It largely inspired by [SmallRye Reactive Messaging implementation](https://github.com/smallrye/smallrye-reactive-messaging).

### Warning

It's not a full-blown MQTT server, it's built on top of
[Vert.x MQTT Server](https://github.com/vert-x3/vertx-mqtt) and handles only publish requests of 
any QoS. The persistence part of the MQTT server is to be handled by the user of this lib.

## Built with

- [Apache Vert.x](https://vertx.io)
- [Eclipse MicroProfile](https://microprofile.io/) CDI and config
- [RX Java 2](https://github.com/ReactiveX/RxJava)

