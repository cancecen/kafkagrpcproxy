package server.discovery;

import javax.inject.Inject;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperEndpointDiscoverer implements EndpointDiscoverer {

  private final ClusterEndpoints clusterEndpoints;
  private final ZookeeperWatcher zookeeperWatcher;
  private final ZooKeeper zooKeeper;

  @Inject
  public ZookeeperEndpointDiscoverer(
      final String zookeeperEndpoint, final ClusterEndpoints initialEndpoints) {
    try {
      this.clusterEndpoints = initialEndpoints;
      this.zooKeeper = new ZooKeeper(zookeeperEndpoint, 300000, null);
      this.zookeeperWatcher = new ZookeeperWatcher(this.zooKeeper, "/endpoints", initialEndpoints);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getEndpointFor(String topic, String userId) {
    return this.clusterEndpoints.getEndpointFor(topic, userId);
  }
}
