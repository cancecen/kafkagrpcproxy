import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Scanner;
import org.cecen.demo.KafkaProxyServiceGrpc;
import org.cecen.demo.ProduceRequest;
import org.cecen.demo.ProduceResponse;
import org.cecen.demo.RegisterProducerRequest;
import org.cecen.demo.RegisterProducerResponse;

public class Client {

  public static void main(String[] args) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", 9999).usePlaintext().build();
    KafkaProxyServiceGrpc.KafkaProxyServiceBlockingStub stub =
        KafkaProxyServiceGrpc.newBlockingStub(channel);

    RegisterProducerResponse registerClientResponse =
        stub.registerProducer(RegisterProducerRequest.newBuilder().build());
    final String clientId = registerClientResponse.getClientId();
    System.out.println("I am client: " + clientId);

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
                  .setTimestamp(System.currentTimeMillis())
                  .setMessageContent(String.valueOf(msg))
                  .build());
      System.out.println("Response from Kafka Proxy: " + response.getResponseCode().toString());
    }

    channel.shutdown();
  }
}
