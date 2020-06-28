package server.kafkautils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.discovery.EndpointDiscoverer;

public class KafkaProducerWrapper extends ClosableKafkaClient {
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class);

  private final String topic;
  private final String userId;
  private final EndpointDiscoverer endpointDiscoverer;
  private final KafkaClientFactory kafkaClientFactory;

  private KafkaProducer<byte[], byte[]> producer;
  private String endpointInUse;

  public KafkaProducerWrapper(
      final String topic,
      final String userId,
      final EndpointDiscoverer endpointDiscoverer,
      final KafkaClientFactory kafkaClientFactory) {
    this.endpointDiscoverer = endpointDiscoverer;
    this.topic = topic;
    this.userId = userId;
    this.maximumUnusedMillis = 3000000L; // TODO: Make configurable
    this.endpointInUse = endpointDiscoverer.getEndpointFor(topic, userId);
    this.kafkaClientFactory = kafkaClientFactory;
    this.producer = this.kafkaClientFactory.getProducer(this.endpointInUse);
    this.updateLastUsedMillis();
  }

  public RecordMetadata produceAndAckClient(byte[] key, byte[] message) {
    try {
      this.updateLastUsedMillis();
      this.updateIfNotValid();
      return this.producer.send(new ProducerRecord<>(this.topic, key, message)).get();
    } catch (final Exception e) {
      logger.error("Exception while waiting for acknowledgements: ", e);
    }
    return null;
  }

  public void produceAndAckProxy(byte[] key, byte[] message) {
    try {
      this.updateLastUsedMillis();
      this.updateIfNotValid();
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

  private void updateIfNotValid() {
    final String currentEndpoint = endpointDiscoverer.getEndpointFor(this.topic, this.userId);
    if (!this.endpointInUse.equals(currentEndpoint)) {
      this.endpointInUse = currentEndpoint;
      this.producer.close();
      this.producer = this.kafkaClientFactory.getProducer(this.endpointInUse);
    }
  }

  @Override
  public void close() {
    this.producer.close();
  }
}
