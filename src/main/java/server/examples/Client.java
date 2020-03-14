package server.examples;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Scanner;
import org.kafkagrpcproxy.KafkaMessage;
import org.kafkagrpcproxy.KafkaProxyServiceGrpc;
import org.kafkagrpcproxy.ProduceRequest;
import org.kafkagrpcproxy.ProduceResponse;
import org.kafkagrpcproxy.RegisterProducerRequest;
import org.kafkagrpcproxy.RegisterProducerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  public static void main(String[] args) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", 9999).usePlaintext().build();
    KafkaProxyServiceGrpc.KafkaProxyServiceBlockingStub stub =
        KafkaProxyServiceGrpc.newBlockingStub(channel);

    RegisterProducerResponse registerClientResponse =
        stub.registerProducer(RegisterProducerRequest.newBuilder().build());
    final String clientId = registerClientResponse.getClientId();
    logger.info("I am client: " + clientId);

    Metadata.Key<String> clientIdKey = Metadata.Key.of("clientId", ASCII_STRING_MARSHALLER);
    Metadata fixedHeaders = new Metadata();
    fixedHeaders.put(clientIdKey, clientId);
    stub = MetadataUtils.attachHeaders(stub, fixedHeaders);
    Scanner in = new Scanner(System.in);
    while (in.hasNextLine()) {
      String msg = in.nextLine();
      ProduceResponse response =
          stub.produce(
              ProduceRequest.newBuilder()
                  .setMessage(
                      KafkaMessage.newBuilder().setMessageKey("key").setMessageContent(msg).build())
                  .build());
      logger.info(
          "Response from Kafka server.Proxy: "
              + response.getResponseCode().toString()
              + " "
              + response.getOffset());
    }

    channel.shutdown();
  }
}
