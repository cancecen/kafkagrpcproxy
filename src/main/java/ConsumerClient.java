import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import org.cecen.demo.ConsumeRequest;
import org.cecen.demo.ConsumeResponse;
import org.cecen.demo.KafkaMessage;
import org.cecen.demo.KafkaProxyServiceGrpc;
import org.cecen.demo.RegisterConsumerRequest;
import org.cecen.demo.RegisterConsumerResponse;

public class ConsumerClient {

    public static void main(String[] args) {
        ManagedChannel channel =
                ManagedChannelBuilder.forAddress("localhost", 9999).usePlaintext().build();
        KafkaProxyServiceGrpc.KafkaProxyServiceBlockingStub stub =
                KafkaProxyServiceGrpc.newBlockingStub(channel);

        RegisterConsumerResponse registerConsumerResponse =
                stub.registerConsumer(RegisterConsumerRequest.newBuilder()
                        .setAppId("testapp")
                        .build());
        final String clientId = registerConsumerResponse.getClientId();
        System.out.println("I am client: " + clientId);

        Metadata.Key<String> clientIdKey = Metadata.Key.of("clientId", ASCII_STRING_MARSHALLER);
        Metadata fixedHeaders = new Metadata();
        fixedHeaders.put(clientIdKey, clientId);
        stub = MetadataUtils.attachHeaders(stub, fixedHeaders);
        while(true) {
            ConsumeResponse consumeResponse = stub.consume(ConsumeRequest.newBuilder().build());
            System.out.println("Consumed: " + consumeResponse.getMessagesCount() + " messages");
            for (KafkaMessage message: consumeResponse.getMessagesList()) {
                long ts = System.currentTimeMillis();
                System.out.println("Msg: " + message.getMessageContent());
                long latency = ts - message.getTimestamp();
                System.out.println("Latency: " + latency);
            }
        }
    }
}
