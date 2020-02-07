import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.cecen.demo.*;

import java.util.Scanner;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class Client {
    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9999)
                .usePlaintext()
                .build();
        KafkaProxyServiceGrpc.KafkaProxyServiceBlockingStub stub =
                KafkaProxyServiceGrpc.newBlockingStub(channel);

        RegisterProducerResponse registerProducerResponse = stub.registerProducer(RegisterProducerRequest.newBuilder()
                .build());
        final String clientId = registerProducerResponse.getClientId();
        System.out.println("I am client: " + clientId);

        Metadata.Key<String> CLIENT_ID = Metadata.Key.of("clientId", ASCII_STRING_MARSHALLER);
        Metadata fixedHeaders = new Metadata();
        fixedHeaders.put(CLIENT_ID, clientId);
        stub = MetadataUtils.attachHeaders(stub, fixedHeaders);
        Scanner in = new Scanner(System.in);
        while(in.hasNextLine()) {
            String msg = in.nextLine();
            ProduceResponse response = stub.produce(ProduceRequest.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setMessageContent(String.valueOf(msg))
                    .build());
            System.out.println("Response from Kafka Proxy: " + response.getResponseCode().toString());
        }

        channel.shutdown();
    }
}