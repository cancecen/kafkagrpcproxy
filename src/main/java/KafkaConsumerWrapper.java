import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerWrapper {

    private final String topic;
    private final Integer partitionId;
    private final KafkaConsumer<Integer, byte[]> consumer;

    public KafkaConsumerWrapper(final String bootstrapServers, final String topic, final Integer partitionId) {
        this.topic = topic;
        this.partitionId = partitionId;
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<Integer, byte[]>(kafkaConfig);
    }
}
