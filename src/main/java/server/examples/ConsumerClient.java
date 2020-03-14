package server.examples;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.kafkagrpcproxy.ConsumeRequest;
import org.kafkagrpcproxy.ConsumeResponse;
import org.kafkagrpcproxy.KafkaMessage;
import org.kafkagrpcproxy.KafkaProxyServiceGrpc;
import org.kafkagrpcproxy.RegisterConsumerRequest;
import org.kafkagrpcproxy.RegisterConsumerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerClient {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);

  public static void main(String[] args) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", 9999).usePlaintext().build();
    KafkaProxyServiceGrpc.KafkaProxyServiceBlockingStub stub =
        KafkaProxyServiceGrpc.newBlockingStub(channel);

    RegisterConsumerResponse registerConsumerResponse =
        stub.registerConsumer(RegisterConsumerRequest.newBuilder().setGroupId("testapp").build());
    final String clientId = registerConsumerResponse.getClientId();
    logger.info("I am client: " + clientId);

    Metadata.Key<String> clientIdKey = Metadata.Key.of("clientId", ASCII_STRING_MARSHALLER);
    Metadata fixedHeaders = new Metadata();
    fixedHeaders.put(clientIdKey, clientId);
    stub = MetadataUtils.attachHeaders(stub, fixedHeaders);
    while (true) {
      ConsumeResponse consumeResponse = stub.consume(ConsumeRequest.newBuilder().build());
      logger.info("Consumed: " + consumeResponse.getMessagesCount() + " messages");
      for (KafkaMessage message : consumeResponse.getMessagesList()) {
        long ts = System.currentTimeMillis();
        logger.info("Msg: " + message.getMessageContent());
        long latency = ts - message.getTimestamp();
        logger.info("Latency: " + latency);
      }
    }
  }
}
