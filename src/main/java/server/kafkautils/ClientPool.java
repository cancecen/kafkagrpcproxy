package server.kafkautils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// TODO: Potentially separate runnable from this.
public class ClientPool implements Runnable {
  private ConcurrentHashMap<String, KafkaProducerWrapper> kafkaProducers;
  private ConcurrentHashMap<String, KafkaConsumerWrapper> kafkaConsumers;
  private ScheduledExecutorService scheduledExecutorService;

  public ClientPool() {
    kafkaProducers = new ConcurrentHashMap<>();
    kafkaConsumers = new ConcurrentHashMap<>();
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
      final String clientId, final String clusterAddress, final String topic, final String appId) {
    final KafkaConsumerWrapper newConsumer = new KafkaConsumerWrapper(clusterAddress, topic, appId);
    kafkaConsumers.put(clientId, newConsumer);
    return newConsumer;
  }

  public KafkaProducerWrapper createProducerForClient(
      final String clientId, final String clusterAddress, final String topic) {
    final KafkaProducerWrapper newProducer = new KafkaProducerWrapper(clusterAddress, topic);
    kafkaProducers.put(clientId, newProducer);
    return newProducer;
  }

  // TODO: use a concurrent LRU cache. i.e.
  // https://github.com/ben-manes/concurrentlinkedhashmap/wiki/Design
  public void run() {
    System.out.println("Cleaning clients...");
    for (final Map.Entry<String, KafkaConsumerWrapper> consumer : kafkaConsumers.entrySet()) {
      if (consumer.getValue().pastDeadline()) {
        System.out.println("Consumer " + consumer.getKey() + " was past deadline, removing it.");
        consumer.getValue().close();
        kafkaConsumers.remove(consumer.getKey());
      }
    }

    for (final Map.Entry<String, KafkaProducerWrapper> producer : kafkaProducers.entrySet()) {
      if (producer.getValue().pastDeadline()) {
        System.out.println("Producer " + producer.getKey() + " was past deadline, removing it.");
        producer.getValue().close();
        kafkaProducers.remove(producer.getKey());
      }
    }
  }
}
