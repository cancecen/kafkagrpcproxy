package server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.interceptors.ClientIdInterceptor;

public class Proxy {
  private static Logger logger = LoggerFactory.getLogger(Proxy.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    logger.info("Starting server...");
    Server server =
        ServerBuilder.forPort(9999)
            .addService(
                ServerInterceptors.intercept(
                    new KafkaProxyServiceImpl(), new ClientIdInterceptor()))
            .build();

    server.start();
    logger.info("Server started successfully...");
    server.awaitTermination();
  }
}
