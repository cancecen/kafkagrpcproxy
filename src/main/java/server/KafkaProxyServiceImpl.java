package server;

import io.grpc.stub.StreamObserver;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.constants.Constants;
import server.kafkautils.ClientPool;
import server.kafkautils.KafkaConsumerWrapper;
import server.kafkautils.KafkaProducerWrapper;

public class KafkaProxyServiceImpl extends KafkaProxyServiceGrpc.KafkaProxyServiceImplBase {

  private static final Logger logger = LoggerFactory.getLogger(KafkaProxyServiceImpl.class);

  private ClientPool clientPool;

  public KafkaProxyServiceImpl() {
    clientPool = new ClientPool();
  }

  @Override
  public void registerProducer(
      final RegisterProducerRequest request,
      final StreamObserver<RegisterProducerResponse> responseObserver) {
    final String uuid = UUID.randomUUID().toString();
    clientPool.createProducerForClient(uuid, "127.0.0.1:9092", "test");

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
    clientPool.createConsumerForClient(uuid, "127.0.0.1:9092", "test", request.getAppId());

    final RegisterConsumerResponse response =
        RegisterConsumerResponse.newBuilder().setClientId(uuid).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void produce(
      final ProduceRequest request, final StreamObserver<ProduceResponse> responseObserver) {
    final String clientId = Constants.CLIENT_ID_KEY.get();
    logger.info("Client calling me is " + clientId);

    final KafkaProducerWrapper producerWrapper =
        clientPool.getProducerForClient(clientId); // handle null
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
    logger.info("Client calling me is " + clientId);
    final KafkaConsumerWrapper consumerWrapper = clientPool.getConsumerForClient(clientId);
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
