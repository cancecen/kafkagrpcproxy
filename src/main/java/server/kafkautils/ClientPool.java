package server.kafkautils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.discovery.EndpointDiscoverer;

// TODO: Potentially separate runnable from this.
@Singleton
public class ClientPool implements Runnable {
  private Logger logger = LoggerFactory.getLogger(ClientPool.class);
  private final ConcurrentHashMap<String, KafkaProducerWrapper> kafkaProducers;
  private final ConcurrentHashMap<String, KafkaConsumerWrapper> kafkaConsumers;
  private final EndpointDiscoverer endpointDiscoverer;
  private final ScheduledExecutorService scheduledExecutorService;
  private final KafkaClientFactory kafkaClientFactory;

  @Inject
  public ClientPool(
      final EndpointDiscoverer endpointDiscoverer, final KafkaClientFactory kafkaClientFactory) {
    this.kafkaProducers = new ConcurrentHashMap<>();
    this.kafkaConsumers = new ConcurrentHashMap<>();
    this.endpointDiscoverer = endpointDiscoverer;
    this.kafkaClientFactory = kafkaClientFactory;
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
  }

  public KafkaConsumerWrapper getConsumerForClient(final String clientId) {
    return kafkaConsumers.get(clientId);
  }

  public KafkaProducerWrapper getProducerForClient(final String clientId) {
    return kafkaProducers.get(clientId);
  }

  public KafkaConsumerWrapper createConsumerForClient(
      final String clientId, final String topic, final String appId) {
    final KafkaConsumerWrapper newConsumer =
        new KafkaConsumerWrapper(topic, appId, "user_id", endpointDiscoverer);
    kafkaConsumers.put(clientId, newConsumer);
    return newConsumer;
  }

  public KafkaProducerWrapper createProducerForClient(final String clientId, final String topic) {
    final KafkaProducerWrapper newProducer =
        new KafkaProducerWrapper(topic, "user_id", endpointDiscoverer, kafkaClientFactory);
    kafkaProducers.put(clientId, newProducer);
    return newProducer;
  }

  // TODO: use a concurrent LRU cache. i.e.
  // https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design
  public void run() {
    logger.info("Cleaning clients...");
    for (final Map.Entry<String, KafkaConsumerWrapper> consumer : kafkaConsumers.entrySet()) {
      if (consumer.getValue().pastDeadline()) {
        logger.info("Consumer " + consumer.getKey() + " was past deadline, removing it.");
        consumer.getValue().close();
        kafkaConsumers.remove(consumer.getKey());
      }
    }

    for (final Map.Entry<String, KafkaProducerWrapper> producer : kafkaProducers.entrySet()) {
      if (producer.getValue().pastDeadline()) {
        logger.info("Producer " + producer.getKey() + " was past deadline, removing it.");
        producer.getValue().close();
        kafkaProducers.remove(producer.getKey());
      }
    }
  }
}
