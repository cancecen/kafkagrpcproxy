package server;

import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.cecen.demo.ConsumeRequest;
import org.cecen.demo.ConsumeResponse;
import org.cecen.demo.KafkaMessage;
import org.cecen.demo.KafkaProxyServiceGrpc;
import org.cecen.demo.ProduceRequest;
import org.cecen.demo.ProduceResponse;
import org.cecen.demo.RegisterConsumerRequest;
import org.cecen.demo.RegisterConsumerResponse;
import org.cecen.demo.RegisterProducerRequest;
import org.cecen.demo.RegisterProducerResponse;
import org.cecen.demo.ResponseCode;
import server.constants.Constants;
import server.kafkautils.KafkaConsumerWrapper;
import server.kafkautils.KafkaProducerWrapper;

public class KafkaProxyServiceImpl extends KafkaProxyServiceGrpc.KafkaProxyServiceImplBase {

  private final Map<String, KafkaProducerWrapper> producerClientPool;
  private final Map<String, KafkaConsumerWrapper> consumerClientPool;

  public KafkaProxyServiceImpl() {
    producerClientPool = new HashMap<>();
    consumerClientPool = new HashMap<>();
  }

  @Override
  public void registerProducer(
      final RegisterProducerRequest request,
      final StreamObserver<RegisterProducerResponse> responseObserver) {
    final String uuid = UUID.randomUUID().toString();
    producerClientPool.put(uuid, new KafkaProducerWrapper("127.0.0.1:9092", "test"));

    final RegisterProducerResponse response =
        RegisterProducerResponse.newBuilder().setClientId(uuid).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void registerConsumer(
      final RegisterConsumerRequest request,
      final StreamObserver<RegisterConsumerResponse> responseObserver) {
    final String uuid = UUID.randomUUID().toString();
    consumerClientPool.put(
        uuid, new KafkaConsumerWrapper("127.0.0.1:9092", "test", request.getAppId()));

    final RegisterConsumerResponse response =
        RegisterConsumerResponse.newBuilder().setClientId(uuid).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void produce(
      final ProduceRequest request, final StreamObserver<ProduceResponse> responseObserver) {
    final String clientId = Constants.CLIENT_ID_KEY.get();
    System.out.println("server.examples.Client calling me is " + clientId);
    final KafkaProducerWrapper producerWrapper = producerClientPool.get(clientId); // handle null
    producerWrapper.produce(
        request.getMessage().getMessageKey().getBytes(),
        request.getMessage().getMessageContent().getBytes());
    final ProduceResponse response =
        ProduceResponse.newBuilder().setResponseCode(ResponseCode.OK).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void consume(
      final ConsumeRequest request, final StreamObserver<ConsumeResponse> responseObserver) {
    final String clientId = Constants.CLIENT_ID_KEY.get();
    System.out.println("server.examples.Client calling me is " + clientId);
    final KafkaConsumerWrapper consumerWrapper =
        consumerClientPool.get(clientId); // TODO: handle null
    ConsumeResponse.Builder consumeResponseBuilder = ConsumeResponse.newBuilder();

    consumerWrapper
        .getRecords()
        .forEach(
            msg ->
                consumeResponseBuilder.addMessages(
                    KafkaMessage.newBuilder()
                        .setMessageKey(new String(msg.key()))
                        .setMessageContent(new String(msg.value()))
                        .setTimestamp(msg.timestamp())
                        .build()));
    responseObserver.onNext(consumeResponseBuilder.build());
    responseObserver.onCompleted();
  }
}
