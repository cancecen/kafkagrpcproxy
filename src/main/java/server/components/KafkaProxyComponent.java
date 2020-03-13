package server.components;

import dagger.Component;
import javax.inject.Singleton;
import server.KafkaProxyServiceImpl;
import server.modules.KafkaProxyModule;

@Singleton
@Component(modules = {KafkaProxyModule.class})
public interface KafkaProxyComponent {
  KafkaProxyServiceImpl kafkaProxyService();
}
