package server;

import io.grpc.stub.StreamObserver;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kafkagrpcproxy.ConsumeRequest;
import org.kafkagrpcproxy.ConsumeResponse;
import org.kafkagrpcproxy.GetAcksAt;
import org.kafkagrpcproxy.KafkaMessage;
import org.kafkagrpcproxy.KafkaProxyServiceGrpc;
import org.kafkagrpcproxy.ProduceRequest;
import org.kafkagrpcproxy.ProduceResponse;
import org.kafkagrpcproxy.RegisterConsumerRequest;
import org.kafkagrpcproxy.RegisterConsumerResponse;
import org.kafkagrpcproxy.RegisterProducerRequest;
import org.kafkagrpcproxy.RegisterProducerResponse;
import org.kafkagrpcproxy.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.constants.Constants;
import server.kafkautils.ClientPool;
import server.kafkautils.KafkaConsumerWrapper;
import server.kafkautils.KafkaProducerWrapper;

@Singleton
public class KafkaProxyServiceImpl extends KafkaProxyServiceGrpc.KafkaProxyServiceImplBase {

  private static final Logger logger = LoggerFactory.getLogger(KafkaProxyServiceImpl.class);

  private ClientPool clientPool;

  @Inject
  public KafkaProxyServiceImpl(final ClientPool clientPool) {
    this.clientPool = clientPool;
  }

  @Override
  public void registerProducer(
      final RegisterProducerRequest request,
      final StreamObserver<RegisterProducerResponse> responseObserver) {
    final String uuid = UUID.randomUUID().toString();
    clientPool.createProducerForClient(uuid, "test");

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
    clientPool.createConsumerForClient(uuid, "test", request.getGroupId());

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
    final ProduceResponse response;
    final byte[] key = request.getMessage().getMessageKey().getBytes();
    final byte[] messageContent = request.getMessage().getMessageContent().getBytes();

    if (request.getGetAcksAt().equals(GetAcksAt.CLIENT)) {
      final RecordMetadata kafkaResponse = producerWrapper.produceAndAckClient(key, messageContent);
      response =
          ProduceResponse.newBuilder()
              .setResponseCode(ResponseCode.OK)
              .setOffset(kafkaResponse.offset())
              .build();
    } else {
      producerWrapper.produceAndAckProxy(key, messageContent);
      response = ProduceResponse.newBuilder().setResponseCode(ResponseCode.OK).build();
    }
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
