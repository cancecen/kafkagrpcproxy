package server.kafkautils;

public abstract class ClosableKafkaClient {
  private long lastUsedMillis;
  protected long maximumUnusedMillis;

  public void updateLastUsedMillis() {
    this.lastUsedMillis = System.currentTimeMillis();
  }

  public boolean pastDeadline() {
    return System.currentTimeMillis() - lastUsedMillis > maximumUnusedMillis;
  }

  public abstract void close();
}
