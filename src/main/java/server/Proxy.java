package server;

import com.be_hase.grpc.micrometer.GrpcMetricsConfigure;
import com.be_hase.grpc.micrometer.MicrometerServerInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.components.DaggerKafkaProxyComponent;
import server.components.KafkaProxyComponent;
import server.interceptors.ClientIdInterceptor;

public class Proxy {
  private static Logger logger = LoggerFactory.getLogger(Proxy.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    logger.info("Starting server...");
    final GrpcMetricsConfigure configure =
        GrpcMetricsConfigure.create()
            .withLatencyTimerConfigure(
                builder -> {
                  builder.publishPercentiles(0.5, 0.75, 0.95, 0.99);
                });

    final KafkaProxyComponent kafkaProxyComponent = DaggerKafkaProxyComponent.builder().build();
    Server server =
        ServerBuilder.forPort(9999)
            .addService(
                ServerInterceptors.intercept(
                    kafkaProxyComponent.kafkaProxyService(),
                    new ClientIdInterceptor(),
                    new MicrometerServerInterceptor(Metrics.globalRegistry, configure)))
            .build();
    server.start();
    logger.info("Server started successfully...");
    server.awaitTermination();
  }
}
