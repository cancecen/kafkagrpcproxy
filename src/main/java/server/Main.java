package server;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.components.DaggerKafkaProxyComponent;
import server.components.KafkaProxyComponent;

public class Main {
  private static Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    final KafkaProxyComponent kafkaProxyComponent = DaggerKafkaProxyComponent.builder().build();
    final ProxyServer server = kafkaProxyComponent.proxyServer();

    server.start();
  }
}
