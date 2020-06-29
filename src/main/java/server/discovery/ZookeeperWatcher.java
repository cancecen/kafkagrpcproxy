package server.discovery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperWatcher implements Watcher {
  private Logger logger = LoggerFactory.getLogger(ZookeeperWatcher.class);

  private final ZooKeeper zooKeeper;
  private final ClusterEndpoints clusterEndpoints;
  private final String zkNode;

  public ZookeeperWatcher(
      final ZooKeeper zooKeeper, final String basePath, final ClusterEndpoints clusterEndpoints)
      throws IOException, KeeperException, InterruptedException {
    this.zooKeeper = zooKeeper;
    this.clusterEndpoints = clusterEndpoints;
    this.zkNode = basePath;
    if (this.zooKeeper.exists(zkNode, false) == null) {
      throw new IOException(String.format("The base path %s does not exist in zookeeper", zkNode));
    }
    this.zooKeeper.addWatch(this.zkNode, this, AddWatchMode.PERSISTENT_RECURSIVE);
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    final String path = watchedEvent.getPath();
    if (watchedEvent.getType() == Event.EventType.NodeDataChanged
        || watchedEvent.getType() == Event.EventType.NodeCreated
        && watchedEvent.getPath().startsWith(zkNode)) {
      logger.info("Endpoint changed, updating it");
      final String[] parts = path.split("\\/");
      if (parts.length != 4) {
        return;
      }
      final String topic = parts[2];
      final String user = parts[3];
      final String endpoint;
      try {
        endpoint = new String(zooKeeper.getData(path, false, null), StandardCharsets.US_ASCII);
        logger.info("Endpoint for topic: {}, user{}, changed from {}, to {}",
                topic, user, this.clusterEndpoints.getEndpointFor(topic, user), endpoint);
        this.clusterEndpoints.updateEndpointForTopicAndUser(topic, user, endpoint);
      } catch (KeeperException e) {
        logger.error("Keeper exception: ", e);
      } catch (InterruptedException e) {
        logger.error("Interrupted exception: ", e);
      }
    } else {
      logger.warn("We are not interested in this event.");
    }
  }
}
