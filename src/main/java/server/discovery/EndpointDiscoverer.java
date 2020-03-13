package server.discovery;

public interface EndpointDiscoverer {
  String getEndpointFor(String topic, String userId);
}
