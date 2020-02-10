import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.cecen.demo.KafkaProxyServiceGrpc;
import org.cecen.demo.ProduceRequest;
import org.cecen.demo.ProduceResponse;
import org.cecen.demo.RegisterProducerRequest;
import org.cecen.demo.RegisterProducerResponse;
import org.cecen.demo.ResponseCode;

public class KafkaProxyServiceImpl extends KafkaProxyServiceGrpc.KafkaProxyServiceImplBase {

  private final Map<String, KafkaProducerWrapper> producerClientPool;

  public KafkaProxyServiceImpl() {
    producerClientPool = new HashMap<>();
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
  public void produce(
      final ProduceRequest request, final StreamObserver<ProduceResponse> responseObserver) {
    final String clientId = Constants.CLIENT_ID_KEY.get();
    System.out.println("Client calling me is " + clientId);
    final KafkaProducerWrapper producerWrapper = producerClientPool.get(clientId); // handle null
    producerWrapper.produce(request.getMessageContentBytes().toByteArray());
    final ProduceResponse response =
        ProduceResponse.newBuilder().setResponseCode(ResponseCode.OK).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
