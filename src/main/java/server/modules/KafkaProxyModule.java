package server.modules;

import dagger.Module;
import dagger.Provides;
import java.nio.file.Paths;
import server.discovery.EndpointDiscoverer;
import server.discovery.FileBasedEndpointDiscoverer;
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
}
