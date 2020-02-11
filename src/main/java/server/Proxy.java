package server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import java.io.IOException;
import server.interceptors.ClientIdInterceptor;

public class Proxy {
  public static void main(String[] args) throws IOException, InterruptedException {
    Server server =
        ServerBuilder.forPort(9999)
            .addService(
                ServerInterceptors.intercept(
                    new KafkaProxyServiceImpl(), new ClientIdInterceptor()))
            .build();

    server.start();
    server.awaitTermination();
  }
}
