package server.kafkautils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaConsumerWrapper {
  // TODO: make configurable
  private static final int MAX_POLL_MS = 100;

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
    this.consumer = new KafkaConsumer<>(kafkaConfig);
    this.consumer.subscribe(Arrays.asList(topic));
  }

  // TODO: make this return and object not tied to Kafka client implementation
  public ConsumerRecords<byte[], byte[]> getRecords() {
    return this.consumer.poll(Duration.ofMillis(MAX_POLL_MS));
  }
}
