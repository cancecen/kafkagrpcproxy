package server.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FileBasedEndpointDiscoverer implements EndpointDiscoverer {
  private Logger logger = LoggerFactory.getLogger(FileBasedEndpointDiscoverer.class);

  private final Path pathToWatch;
  private final Path pathToLoad;
  private final WatchService watchService;
  private final Thread fileWatcher;
  private ObjectMapper objectMapper;
  private ClusterEndpoints endpoints;

  @Inject
  public FileBasedEndpointDiscoverer(final Path pathToWatch, final String filename) {
    this.pathToWatch = pathToWatch;
    this.pathToLoad = Paths.get(pathToWatch.toString(), filename);
    try {
      this.watchService = FileSystems.getDefault().newWatchService();
      this.pathToWatch.register(
          this.watchService,
          StandardWatchEventKinds.ENTRY_CREATE,
          StandardWatchEventKinds.ENTRY_MODIFY);
    } catch (IOException ex) {
      throw new RuntimeException("Cannot set a watcher for endpoint discoverer: ", ex);
    }
    updateFile();

    fileWatcher =
        new Thread(
            () -> {
              while (true) {
                try {
                  watchService.take();
                  updateFile();
                } catch (InterruptedException ex) {
                  return;
                }
              }
            });
    fileWatcher.start();
  }

  @Override
  public String getEndpointFor(String topic, String userId) {
    return endpoints.getEndpointFor(topic, userId);
  }

  private synchronized void updateFile() {
    try {
      objectMapper = new ObjectMapper(new YAMLFactory());
      endpoints = objectMapper.readValue(pathToLoad.toFile(), ClusterEndpoints.class);
    } catch (final IOException ex) {
      throw new RuntimeException("Error while loading the endpoint file: ", ex);
    }
  }
}
