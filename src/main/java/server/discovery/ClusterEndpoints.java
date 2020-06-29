package server.discovery;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterEndpoints {
  private Logger logger = LoggerFactory.getLogger(ClusterEndpoints.class);

  private Map<String, Map<String, String>> topics;

  public ClusterEndpoints() {
    topics = new ConcurrentHashMap<>();
  }

  public void updateEndpointForTopicAndUser(
      final String topic, final String user, final String endpoint) {
    Map<String, String> topicMap = this.topics.getOrDefault(topic, new ConcurrentHashMap<>());
    topicMap.put(user, endpoint);
    topics.put(topic, topicMap);
  }

  public void setTopics(final Map<String, Map<String, String>> topics) {
    this.topics = topics;
  }

  public String getEndpointFor(final String topic, final String userId) {
    if (!topics.containsKey(topic) || !topics.get(topic).containsKey(userId)) {
      logger.warn("No endpoint registered for topic {} and user id {}", topic, userId);
      return null;
    }
    return topics.get(topic).get(userId);
  }
}
