import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerWrapper {

    private final String topic;
    private final KafkaProducer<Integer, byte[]> producer;

    public KafkaProducerWrapper(final String destination, final String topic) {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destination);
        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<Integer, byte[]>(kafkaConfig);
        this.topic = topic;
    }

    public void produce(byte[] message) {
        producer.send(new ProducerRecord<>(this.topic, 1, message),
                (metadata, e) -> {
                    if (e != null) {
                        e.printStackTrace();

                    } else {
                        System.out.println("The offset of the record we just sent is: " + metadata.offset() +
                                "with size: " + metadata.serializedValueSize());
                    }
                });
    }
}
