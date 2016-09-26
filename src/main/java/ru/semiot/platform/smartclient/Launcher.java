package ru.semiot.platform.smartclient;

import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.semiot.platform.smartclient.parsers.AverageValueParser;
import ru.semiot.platform.smartclient.parsers.ClientResultParser;
import ru.semiot.platform.smartclient.wamp.WAMPClient;
import rx.Observer;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
public class Launcher {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);
  private static final ClientConfig CONFIG = ConfigFactory.create(ClientConfig.class);
  private static final String OPTION_COMMAND = "command";
  private static final String OPTION_INPUT = "input";
  private HashMap<String, HashMap<String, String>> devicesInBuildings = new HashMap<>();
  private HashMap<String, HashMap<String, String>> regulatorsInBuildings = new HashMap<>();
  private HashMap<String, Double> regulatorsLastResults;
  //private HashMap<String, Integer> countsDevicesInBuildings;
  //private HashMap<String, Double> sumTempInBuildings;
  private HashMap<String, Observer<String>> buildingsObservers;
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
    //countsDevicesInBuildings = new HashMap<>();
    //sumTempInBuildings = new HashMap<>();
    buildingsObservers = new HashMap<>();
    try {
      WAMPClient.getInstance().init(CONFIG.wampUrl(), "realm1", 15, CONFIG.hostUsername(), CONFIG.hostPassword());
    } catch (Exception ex) {
      System.out.println("WAMP is bad!");
      logger.error("Unknown exception in init WAMP", ex);
    }
  }

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption(Option.builder(OPTION_COMMAND)
        .hasArg()
        .build());
    options.addOption(Option.builder(OPTION_INPUT)
        .hasArgs()
        .build());
    options.addOption(Option.builder("id")
        .hasArg()
        .build());

    CommandLineParser cliParser = new DefaultParser();
    try {
      CommandLine cliArgs = cliParser.parse(options, args);
      if (cliArgs.hasOption(OPTION_COMMAND)) {
        if (cliArgs.getOptionValue(OPTION_COMMAND).equalsIgnoreCase("parseAndCompute")) {
          if (cliArgs.hasOption(OPTION_INPUT)) {
            String[] filePaths = cliArgs.getOptionValues(OPTION_INPUT);
            if (filePaths.length > 0) {
              Path[] inputs = new Path[filePaths.length];
              for (int i = 0; i < filePaths.length; i++) {
                inputs[i] = Paths.get(filePaths[i]);
              }

              ClientResultParser parser;
              if (cliArgs.hasOption("id")) {
                parser = new ClientResultParser(inputs, cliArgs.getOptionValue("id"));
              } else {
                parser = new ClientResultParser(inputs);
              }

              parser.run();
            } else {
              throw new IllegalArgumentException("input is empty!");
            }
          } else {
            throw new IllegalArgumentException("input");
          }
        } else if (cliArgs.getOptionValue(OPTION_COMMAND).equalsIgnoreCase("countAverage")) {
          if (cliArgs.hasOption(OPTION_INPUT)) {
            String[] filePaths = cliArgs.getOptionValues(OPTION_INPUT);
            if (filePaths.length > 0) {
              for (String filePath : filePaths) {
                logger.info("File: {}", filePath);
                AverageValueParser parser = new AverageValueParser(Paths.get(filePath));
                Double average = parser.call();
                logger.info("Average: {}", average);
              }
            } else {
              throw new IllegalArgumentException(Arrays.toString(cliArgs.getArgs()));
            }
          } else {
            throw new IllegalArgumentException(Arrays.toString(cliArgs.getArgs()));
          }
        } else {
          throw new IllegalArgumentException(Arrays.toString(cliArgs.getArgs()));
        }
      } else {
        Launcher launcher = new Launcher();
        launcher.run();
      }
    } catch (ParseException e) {
      logger.error(e.getMessage(), e);
    }
  }

  public void run() {
    logger.info("Smart-client is starting");
    HTTPClient.getInstance().getDevicesAndRegulators(devicesInBuildings, regulatorsInBuildings);
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
        try {
          regulatorsLastResults.put(regulator,
              HTTPClient.getInstance().getLastCommandResult(regulator));
        } catch (IllegalStateException ex) {
          regulatorsLastResults.put(regulator, 1.0);
        }
      }
    }
    logger.info("Try to subscribe in devices' topics");
    for (String building : devicesInBuildings.keySet()) {
      DeviceObserver o = new DeviceObserver(devicesInBuildings.get(building).size(), building);
      buildingsObservers.put(building, o);
      for (String device : devicesInBuildings.get(building).keySet()) {
        WAMPClient.getInstance().subscribe(devicesInBuildings.get(building).get(device))
            .subscribe(o);
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

  /*private synchronized void appendValue(String device, double value) {
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
  }*/
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
    String query = "SELECT ?ts WHERE { " +
        "?z a <http://purl.oclc.org/NET/ssnx/ssn#Observation>; " +
        "<http://purl.oclc.org/NET/ssnx/ssn#observationResultTime> ?ts }";
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

  private class DeviceObserver implements Observer<String> {

    private final Integer TOTAL_COUNT;
    private final String BUILDING;
    Integer count;
    Double sum;

    public DeviceObserver(int total, String building) {
      TOTAL_COUNT = total;
      this.BUILDING = building;
      count = 0;
      sum = (double) 0;
    }

    @Override
    public void onCompleted() {

    }

    @Override
    public void onError(Throwable e) {
      logger.error("Something went wrong with devices", e);
    }

    @Override
    public void onNext(java.lang.String observation) {
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
      double value = getValueFromModel(observation, QUERY_OBS_VALUE);
      if (count < TOTAL_COUNT) {
        appendValue(value);
        if (count == TOTAL_COUNT) {
          checkTemperature();
        }

      } else {
        sum = (double) 0;
        count = 0;
      }
    }

    private synchronized void appendValue(double v) {
      count++;
      sum += v;
    }

    private void checkTemperature() {
      double avg = sum / count;
      logger.info("Check temperature in building {}. Average temperature is {}", BUILDING, avg);
      if (avg > CONFIG.maxTemperature()) {
        logger.info("Temperature is big!");
        for (java.lang.String regulator : regulatorsInBuildings.get(BUILDING).keySet()) {
          HTTPClient.getInstance().sendCommand(regulator, regulatorsLastResults.get(regulator) - CONFIG.step());
        }
      }
      if (avg < CONFIG.minTemperature()) {
        logger.info("Temperature is small!");
        for (java.lang.String regulator : regulatorsInBuildings.get(BUILDING).keySet()) {
          HTTPClient.getInstance().sendCommand(regulator, regulatorsLastResults.get(regulator) + CONFIG.step());
        }
      }
      sum = (double) 0;
      count = 0;
    }

  }
}
