package server.modules;

import com.be_hase.grpc.micrometer.GrpcMetricsConfigure;
import com.be_hase.grpc.micrometer.MicrometerServerInterceptor;
import dagger.Module;
import dagger.Provides;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.micrometer.core.instrument.Metrics;
import java.nio.file.Paths;
import server.KafkaProxyServiceImpl;
import server.discovery.EndpointDiscoverer;
import server.discovery.FileBasedEndpointDiscoverer;
import server.interceptors.ClientIdInterceptor;
import server.kafkautils.ClientPool;

@Module
public class KafkaProxyModule {

  @Provides
  public static EndpointDiscoverer provideFileBasedEndpointDiscoverer() {
    return new FileBasedEndpointDiscoverer(
        Paths.get("target/resources/cluster-coordinates").toAbsolutePath(),
        "cluster-coordinates.yaml");
  }

  @Provides
  public static ClientPool provideClientPool(final EndpointDiscoverer endpointDiscoverer) {
    return new ClientPool(endpointDiscoverer);
  }

  @Provides
  public static GrpcMetricsConfigure provideGrpcMetricsConfigure() {
    return GrpcMetricsConfigure.create()
        .withLatencyTimerConfigure(
            builder -> {
              builder.publishPercentiles(0.5, 0.75, 0.95, 0.99);
            });
  }

  @Provides
  public static MicrometerServerInterceptor provideMicrometerServerInterceptor(
      final GrpcMetricsConfigure configure) {
    return new MicrometerServerInterceptor(Metrics.globalRegistry, configure);
  }

  @Provides
  public static ClientIdInterceptor provideClientIdInterceptor() {
    return new ClientIdInterceptor();
  }

  @Provides
  public static KafkaProxyServiceImpl provideKafkaProxyServiceImp(final ClientPool clientPool) {
    return new KafkaProxyServiceImpl(clientPool);
  }

  @Provides
  public static Server provideServer(
      final ClientIdInterceptor clientIdInterceptor,
      final MicrometerServerInterceptor micrometerServerInterceptor,
      final KafkaProxyServiceImpl kafkaProxyServiceImpl) {
    return ServerBuilder.forPort(9999)
        .addService(
            ServerInterceptors.intercept(
                kafkaProxyServiceImpl, clientIdInterceptor, micrometerServerInterceptor))
        .build();
  }
}
