package ru.semiot.platform.smart.client;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

/**
 *
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
@LoadPolicy(LoadType.FIRST)
@Sources({"file:/semiot-platform/smart-client/config.properties"})
public interface ClientConfig extends Config {

  @DefaultValue("ws://demo.semiot.ru:8080/ws")
  @Key("smart.client.wamp.url")
  String wampUrl();

  @DefaultValue("root")
  @Key("smart.client.host.username")
  String hostUsername();

  @DefaultValue("root")
  @Key("smart.client.host.password")
  String hostPassword();

  @DefaultValue("https://demo.semiot.ru")
  @Key("smart.client.host.uri")
  String hostUrl();

  @DefaultValue("18")
  @Key("smart.client.temperature.min")
  double minTemperature();

  @DefaultValue("24")
  @Key("smart.client.temperature.max")
  double maxTemperature();

  @DefaultValue("2")
  @Key("smart.client.temperature.step")
  double step();
  
  @DefaultValue("30")
  @Key("smart.client.size.page")
  int sizePage();
}
