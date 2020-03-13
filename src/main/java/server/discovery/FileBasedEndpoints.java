package server.discovery;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedEndpoints {
  private Logger logger = LoggerFactory.getLogger(FileBasedEndpoints.class);

  private Map<String, Map<String, String>> topics;

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
