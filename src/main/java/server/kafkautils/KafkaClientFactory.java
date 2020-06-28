package server.kafkautils;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

@Singleton
public class KafkaClientFactory {

  @Inject
  public KafkaClientFactory() {

  }

  public KafkaProducer<byte[], byte[]> getProducer(final String endpoints) {
    Map<String, Object> kafkaConfig = new HashMap<>();
    kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoints);
    kafkaConfig.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    kafkaConfig.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    return new KafkaProducer<>(kafkaConfig);
  }
}
