package server;

import io.grpc.Server;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ProxyServer {
  private static Logger logger = LoggerFactory.getLogger(ProxyServer.class);

  private final Server server;

  @Inject
  public ProxyServer(final Server server) {
    this.server = server;
  }

  public void start() throws IOException, InterruptedException {
    logger.info("Starting server...");
    server.start();
    logger.info("Server started successfully...");
    server.awaitTermination();
  }
}
