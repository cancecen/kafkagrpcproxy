package server.kafkautils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaConsumerWrapper extends ClosableKafkaClient {
  // TODO: make configurable
  private static final int POLL_FREQ_MS = 1000;

  private final String topic;
  private final KafkaConsumer<byte[], byte[]> consumer;

  public KafkaConsumerWrapper(
      final String bootstrapServers, final String topic, final String appId) {
    this.topic = topic;
    final Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appId);
    kafkaConfig.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    kafkaConfig.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    kafkaConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);
    this.consumer = new KafkaConsumer<>(kafkaConfig);
    this.maximumUnusedMillis = 60000L; // TODO: Make configurable.
    this.consumer.subscribe(Arrays.asList(topic));
    this.updateLastUsedMillis();
  }

  // TODO: make this return and object not tied to Kafka client implementation
  public ConsumerRecords<byte[], byte[]> getRecords() {
    this.updateLastUsedMillis();
    return this.consumer.poll(Duration.ofMillis(POLL_FREQ_MS));
  }

  @Override
  public void close() {
    this.consumer.close();
  }
}
