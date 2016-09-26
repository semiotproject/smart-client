package ru.semiot.platform.smartclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.semiot.platform.smartclient.wamp.WAMPClient;

import java.util.HashMap;
import java.util.Map;

public class BuildingHeatMngRunner implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(BuildingHeatMngRunner.class);
  private static volatile BuildingHeatMngRunner instance;
  private final Map<String, Map<String, String>> sensorsInBuildings = new HashMap<>();
  private final Map<String, Map<String, String>> regulatorsInBuildings = new HashMap<>();
  private final Map<String, BuildingManager> buildings = new HashMap<>();

  private BuildingHeatMngRunner() {}

  public static BuildingHeatMngRunner getInstance() {
    BuildingHeatMngRunner localInstance = instance;
    if (localInstance == null) {
      synchronized (BuildingHeatMngRunner.class) {
        localInstance = instance;
        if (localInstance == null)
          instance = localInstance = new BuildingHeatMngRunner();
      }
    }

    return localInstance;
  }

  @Override
  public void run() {
    logger.info("Smart-client is starting");

    HTTPClient.getInstance().getSensorsAndRegulators(sensorsInBuildings, regulatorsInBuildings);

    logger.info("Subscribing to regulators' command results");

    for (String building : regulatorsInBuildings.keySet()) {
      for (String regulatorId : regulatorsInBuildings.get(building).keySet()) {
        BuildingManager buildingManager = new BuildingManager(building, regulatorId);
        buildings.put(building, buildingManager);

        WAMPClient.getInstance()
            .subscribe(regulatorsInBuildings.get(building).get(regulatorId))
            .subscribe(buildingManager.getRegulatorObserver());

        buildingManager.loadLastRegulatorCommandResult();
      }
    }

    logger.info("Subscribing to sensors' observations");

    for (String building : sensorsInBuildings.keySet()) {
      BuildingManager buildingManager = buildings.get(building);
      buildingManager.setNumberOfSensors(sensorsInBuildings.get(building).size());

      for (String sensor : sensorsInBuildings.get(building).keySet()) {
        WAMPClient.getInstance()
            .subscribe(sensorsInBuildings.get(building).get(sensor))
            .subscribe(buildingManager.getTemperatureObserver());
      }

      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        logger.warn(e.getMessage(), e);
      }
    }

    logger.info("Smart-client is started");

    try {
      while (true) {
        Thread.sleep(120000);
      }
    } catch (InterruptedException ex) {
    }
  }

}
