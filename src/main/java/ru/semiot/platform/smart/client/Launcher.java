package ru.semiot.platform.smart.client;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import org.aeonbits.owner.ConfigFactory;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

/**
 *
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
public class Launcher {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);
  private static final ClientConfig CONFIG = ConfigFactory.create(ClientConfig.class);
  private HashMap<String, HashMap<String, String>> devicesInBuildings;
  private HashMap<String, HashMap<String, String>> regulatorsInBuildings;
  private HashMap<String, Double> regulatorsLastResults;
  private HashMap<String, Integer> countsDevicesInBuildings;
  private HashMap<String, Double> sumTempInBuildings;
  private static final String QUERY_VALUE = "SELECT ?value { "
      + "?z a <http://w3id.org/semiot/ontologies/semiot#MappingParameter>; "
      + "<http://www.loa-cnr.it/ontologies/DUL.owl#hasParameterDataValue> ?value "
      + "}";

  private static final String QUERY_OBS_VALUE = "SELECT ?value { "
      + "?z a <http://qudt.org/schema/qudt#QuantityValue>; "
      + "<http://qudt.org/schema/qudt#quantityValue> ?value "
      + "}";

  public Launcher() {
    logger.debug("Try to initialize launcher");
    HTTPClient.getInstance().init(CONFIG.hostUrl(), CONFIG.hostPassword(), CONFIG.hostUsername());
    regulatorsLastResults = new HashMap<>();
    countsDevicesInBuildings = new HashMap<>();
    sumTempInBuildings = new HashMap<>();
    try {
      WAMPClient.getInstance().init(CONFIG.wampUrl(), "realm1", 15, CONFIG.hostUsername(), CONFIG.hostPassword());
    } catch (Exception ex) {
      System.out.println("WAMP is bad!");
      logger.error("Unknown exception in init WAMP", ex);
    }
  }

  public static void main(String[] argv) throws UnsupportedEncodingException, IOException {
    Launcher launcher = new Launcher();
    launcher.run();
  }

  public void run() {
    logger.info("Smart-client is starting");
    devicesInBuildings = HTTPClient.getInstance().getDevices();
    regulatorsInBuildings = HTTPClient.getInstance().getRegulators();
    logger.debug("Try to subscribe in regulators' topics");
    for (String building : regulatorsInBuildings.keySet()) {
      for (String regulator : regulatorsInBuildings.get(building).keySet()) {
        WAMPClient.getInstance().subscribe(regulatorsInBuildings.get(building).get(regulator))
            .subscribe(new Observer<String>() {
              @Override
              public void onCompleted() {

              }

              @Override
              public void onError(Throwable e) {
                logger.error("Something went wrong with regulator", e);
              }

              @Override
              public void onNext(String commandResult) {
                regulatorsLastResults.put(regulator, getValueFromModel(commandResult, QUERY_VALUE));
              }
            });
        regulatorsLastResults.put(regulator, HTTPClient.getInstance().getLastCommandResult(regulator));
      }
    }
    logger.debug("Try to subscribe in devices' topics");
    for (String building : devicesInBuildings.keySet()) {
      for (String device : devicesInBuildings.get(building).keySet()) {
        WAMPClient.getInstance().subscribe(devicesInBuildings.get(building).get(device))
            .subscribe(new Observer<String>() {
              @Override
              public void onCompleted() {

              }

              @Override
              public void onError(Throwable e) {
                logger.error("Something went wrong with devices", e);
              }

              @Override
              public void onNext(String observation) {
                long stop = System.currentTimeMillis();
                String stopTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .withZone(ZoneOffset.UTC)
                    .format(Instant.ofEpochMilli(stop));

                long start = getTimestamp(observation);
                String startTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .withZone(ZoneOffset.UTC)
                    .format(Instant.ofEpochMilli(start));
                logger.debug("Observation sent in {} long {}", startTime, start);
                logger.debug("Observation received in {} long {}", stopTime, stop);
                logger.info("Observation execute time is {} ms", stop - start);
                appendValue(device, getValueFromModel(observation, QUERY_OBS_VALUE));
              }
            });
      }
    }
    logger.info("Smart-client is started");
    while (true);

  }

  private synchronized void appendValue(String device, double value) {
    String building = findBuilding(device);
    if (sumTempInBuildings.containsKey(building)) {
      sumTempInBuildings.put(building, sumTempInBuildings.get(building) + value);
      countsDevicesInBuildings.put(building, countsDevicesInBuildings.get(building) + 1);
      if (countsDevicesInBuildings.get(building) == devicesInBuildings.get(building).size()) {
        countsDevicesInBuildings.put(building, 0);
        checkTemperature(building);
        sumTempInBuildings.remove(building);
      }
    } else {
      sumTempInBuildings.put(building, value);
      countsDevicesInBuildings.put(building, 1);
    }
  }

  private void checkTemperature(String building) {
    double avg = sumTempInBuildings.get(building) / devicesInBuildings.get(building).size();
    logger.debug("Check temperature in building {}. Average temperature is {}", building, avg);
    if (avg > CONFIG.maxTemperature()) {
      logger.debug("Temperature is big!");
      for (String regulator : regulatorsInBuildings.get(building).keySet()) {
        HTTPClient.getInstance().sendCommand(regulator, regulatorsLastResults.get(regulator) - CONFIG.step());
      }
    }
    if (avg < CONFIG.minTemperature()) {
      logger.debug("Temperature is small!");
      for (String regulator : regulatorsInBuildings.get(building).keySet()) {
        HTTPClient.getInstance().sendCommand(regulator, regulatorsLastResults.get(regulator) + CONFIG.step());
      }
    }
  }

  private String findBuilding(String device) {
    for (String building : devicesInBuildings.keySet()) {
      if (devicesInBuildings.get(building).containsKey(device)) {
        return building;
      }
    }
    return null;
  }

  private double getValueFromModel(String m, String query) {
    Model model = ModelFactory.createDefaultModel();
    model.read(new StringReader(m), null, RDFLanguages.strLangJSONLD);
    ResultSet rs = QueryExecutionFactory.create(query, model).execSelect();
    Double value = null;
    while (rs.hasNext()) {
      QuerySolution solution = rs.next();
      value = Double.parseDouble(solution.getLiteral("?value").getString().replace(',', '.'));
    }
    return value;
  }

  private long getTimestamp(String obesrvation) {
    Model model = ModelFactory.createDefaultModel();
    model.read(new StringReader(obesrvation), null, RDFLanguages.strLangJSONLD);
    String query = "SELECT ?ts WHERE { ?z a <http://purl.oclc.org/NET/ssnx/ssn#Observation>; <http://purl.oclc.org/NET/ssnx/ssn#observationResultTime> ?ts }";
    ResultSet rs = QueryExecutionFactory.create(query, model).execSelect();
    Long res = null;
    while (rs.hasNext()) {
      QuerySolution solution = rs.next();
      res = ZonedDateTime.parse(solution.getLiteral("?ts").getString(),
          DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .toInstant()
          .toEpochMilli();
    }
    return res;
  }
}
