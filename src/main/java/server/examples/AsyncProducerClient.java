package server.examples;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.nio.charset.Charset;
import java.util.Random;
import org.kafkagrpcproxy.KafkaMessage;
import org.kafkagrpcproxy.KafkaProxyServiceGrpc;
import org.kafkagrpcproxy.ProduceRequest;
import org.kafkagrpcproxy.ProduceResponse;
import org.kafkagrpcproxy.RegisterProducerRequest;
import org.kafkagrpcproxy.RegisterProducerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncProducerClient {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  public static void main(String[] args) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", 9999).usePlaintext().build();

    KafkaProxyServiceGrpc.KafkaProxyServiceBlockingStub blockingStub =
        KafkaProxyServiceGrpc.newBlockingStub(channel);
    RegisterProducerResponse registerClientResponse =
        blockingStub.registerProducer(RegisterProducerRequest.newBuilder().build());
    final String clientId = registerClientResponse.getClientId();
    logger.info("I am client: " + clientId);

    KafkaProxyServiceGrpc.KafkaProxyServiceStub stub = KafkaProxyServiceGrpc.newStub(channel);
    Metadata.Key<String> clientIdKey = Metadata.Key.of("clientId", ASCII_STRING_MARSHALLER);
    Metadata fixedHeaders = new Metadata();
    fixedHeaders.put(clientIdKey, clientId);
    stub = MetadataUtils.attachHeaders(stub, fixedHeaders);
    StreamObserver<ProduceResponse> response =
        new StreamObserver<>() {
          @Override
          public void onNext(ProduceResponse produceResponse) {
            logger.info(
                "Got response, offset: "
                    + produceResponse.getOffset()
                    + " "
                    + produceResponse.getResponseCode().toString());
          }

          @Override
          public void onError(Throwable throwable) {
            logger.warn("Got error: ", throwable);
          }

          @Override
          public void onCompleted() {

          }
        };

    while (true) {
      byte[] array = new byte[7]; // length is bounded by 7
      new Random().nextBytes(array);
      final String generatedString = new String(array, Charset.forName("UTF-8"));
      ProduceRequest request =
          ProduceRequest.newBuilder()
              .setMessage(
                  KafkaMessage.newBuilder()
                      .setMessageContent(generatedString)
                      .setMessageKey(generatedString)
                      .setTimestamp(System.currentTimeMillis())
                      .build())
              .build();
      stub.produce(request, response);
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
        break;
      }
    }

    channel.shutdown();
  }
}
