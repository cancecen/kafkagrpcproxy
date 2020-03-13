package server.kafkautils;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.discovery.EndpointDiscoverer;

public class KafkaProducerWrapper extends ClosableKafkaClient {
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class);

  private final String topic;
  private final KafkaProducer<byte[], byte[]> producer;
  private final EndpointDiscoverer endpointDiscoverer;

  public KafkaProducerWrapper(
      final String topic, final String userId, final EndpointDiscoverer endpointDiscoverer) {
    this.endpointDiscoverer = endpointDiscoverer;
    Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    kafkaConfig.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, endpointDiscoverer.getEndpointFor(topic, userId));
    kafkaConfig.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    kafkaConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    this.producer = new KafkaProducer<>(kafkaConfig);
    this.topic = topic;
    this.maximumUnusedMillis = 30000L; // TODO: Make configurable
    this.updateLastUsedMillis();
  }

  public void produce(byte[] key, byte[] message) {
    this.producer.send(
        new ProducerRecord<>(this.topic, key, message),
        (metadata, e) -> {
          if (e != null) {
            e.printStackTrace();

          } else {
            logger.info(
                "The offset of the record we just sent is: "
                    + metadata.offset()
                    + "with size: "
                    + metadata.serializedValueSize());
          }
        });
  }

  @Override
  public void close() {
    this.producer.close();
  }
}
