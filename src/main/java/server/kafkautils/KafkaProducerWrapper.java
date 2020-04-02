package server.kafkautils;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.discovery.EndpointDiscoverer;

public class KafkaProducerWrapper extends ClosableKafkaClient {
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class);

  private final String topic;
  private final String userId;
  private final KafkaProducer<byte[], byte[]> producer;
  private final EndpointDiscoverer endpointDiscoverer;

  public KafkaProducerWrapper(
      final String topic, final String userId, final EndpointDiscoverer endpointDiscoverer) {
    this.endpointDiscoverer = endpointDiscoverer;
    this.topic = topic;
    this.userId = userId;
    this.maximumUnusedMillis = 3000000L; // TODO: Make configurable

    Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    kafkaConfig.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, endpointDiscoverer.getEndpointFor(topic, userId));
    kafkaConfig.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    kafkaConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    this.producer = new KafkaProducer<>(kafkaConfig);
    this.updateLastUsedMillis();
  }

  public RecordMetadata produceAndAckClient(byte[] key, byte[] message) {
    try {
      this.updateLastUsedMillis();
      return this.producer.send(new ProducerRecord<>(this.topic, key, message)).get();
    } catch (final Exception e) {
      logger.error("Exception while waiting for acknowledgements: ", e);
    }
    return null;
  }

  public void produceAndAckProxy(byte[] key, byte[] message) {
    try {
      this.updateLastUsedMillis();
      this.producer.send(
          new ProducerRecord<>(topic, key, message),
          (recordMetadata, e) -> {
            if (e != null) {
              logger.error(
                  "Unable to produce for topic {}, and user {} because of {}",
                  topic,
                  userId,
                  e.getMessage());
              // TODO: Metrics, retries.
            } else {
              logger.debug(
                  "The offset of the record we just sent is {}, with size {}",
                  recordMetadata.offset(),
                  recordMetadata.serializedValueSize());
            }
          });
    } catch (final Exception e) {
      logger.error(
          "Exception while putting record to send buffer for topic {}, reason {} ", topic, e);
    }
  }

  @Override
  public void close() {
    this.producer.close();
  }
}
