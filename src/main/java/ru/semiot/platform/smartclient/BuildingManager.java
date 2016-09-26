package ru.semiot.platform.smartclient;

import org.aeonbits.owner.ConfigFactory;
import org.apache.jena.ext.com.google.common.util.concurrent.AtomicDouble;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.semiot.platform.smartclient.wamp.MultiThreadRDFModelObserver;
import rx.Observer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

public class BuildingManager {

  private static final Logger logger = LoggerFactory.getLogger(BuildingManager.class);
  private static final ClientConfig CONFIG = ConfigFactory.create(ClientConfig.class);
  private static final String QUERY_OBS_VALUE = "SELECT ?value { "
      + "?z a <http://qudt.org/schema/qudt#QuantityValue>; "
      + "<http://qudt.org/schema/qudt#quantityValue> ?value "
      + "}";
  private static final String QUERY_VALUE = "SELECT ?value { " +
      "?z a <http://w3id.org/semiot/ontologies/semiot#MappingParameter>; " +
      "<http://www.loa-cnr.it/ontologies/DUL.owl#hasParameterDataValue> ?value " +
      "}";
  private static final String QUERY_TIMESTAMP = "SELECT ?ts WHERE { " +
      "?z a <http://purl.oclc.org/NET/ssnx/ssn#Observation>; " +
      "<http://purl.oclc.org/NET/ssnx/ssn#observationResultTime> ?ts }";

  private final String building;
  private final String regulatorId;
  private final TemperatureObserver tempObserver;
  private final RegulatorObserver regObserver;
  private final AtomicDouble regulatorLastResult = new AtomicDouble();
  private int numberOfSensors;

  public BuildingManager(String building, String regulatorId) {
    this.building = building;
    this.regulatorId = regulatorId;
    this.tempObserver = new TemperatureObserver();
    this.regObserver = new RegulatorObserver();
  }

  public Observer<String> getTemperatureObserver() {
    return tempObserver;
  }

  public Observer<String> getRegulatorObserver() {
    return regObserver;
  }

  public void loadLastRegulatorCommandResult() {
    try {
      regulatorLastResult.set(HTTPClient.getInstance().getLastCommandResult(regulatorId));
    } catch (IllegalStateException ex) {
      regulatorLastResult.set(1.0);
    }
  }

  public void setNumberOfSensors(int numberOfSensors) {
    this.numberOfSensors = numberOfSensors;
  }

  private double getValueFromModel(Model model, String query) {
    ResultSet rs = QueryExecutionFactory.create(query, model).execSelect();
    Double value = null;
    while (rs.hasNext()) {
      QuerySolution solution = rs.next();
      value = Double.parseDouble(solution.getLiteral("?value").getString().replace(',', '.'));
    }
    return value;
  }

  private class TemperatureObserver extends MultiThreadRDFModelObserver {

    private AtomicInteger count = new AtomicInteger(0);
    private AtomicDouble sum = new AtomicDouble(0.0);

    @Override
    public void onNext(Model observation, long onReceivedTimestamp) {
      String stopTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME
          .withZone(ZoneOffset.UTC)
          .format(Instant.ofEpochMilli(onReceivedTimestamp));

      long start = getObservationTimestamp(observation);
      String startTime = DateTimeFormatter.ISO_OFFSET_DATE_TIME
          .withZone(ZoneOffset.UTC)
          .format(Instant.ofEpochMilli(start));

      logger.debug("Building[{}] Observation sent at {} ({})", building, startTime, start);
      logger.debug("Building[{}] Observation received at {} ({})", building, stopTime, onReceivedTimestamp);
      logger.info("Building[{}] Observation was processed for {} ms", building, onReceivedTimestamp - start);

      double value = getValueFromModel(observation, QUERY_OBS_VALUE);

      if (count.intValue() < numberOfSensors) {
        appendValue(value);
        if (count.intValue() >= numberOfSensors) {
          checkTemperature();

          sum.set(0.0);
          count.set(0);
        }
      }
    }

    private synchronized void appendValue(double value) {
      count.incrementAndGet();
      sum.addAndGet(value);
    }

    private void checkTemperature() {
      double averageValue = sum.doubleValue() / count.intValue();

      logger.debug("Building[{}] Average temperature: {}", building, averageValue);

      if (averageValue > CONFIG.maxTemperature()) {
        logger.info("Building[{}] Temperature is too high! Value: {}", building, averageValue);
        HTTPClient.getInstance()
            .sendCommand(regulatorId, regulatorLastResult.addAndGet(-CONFIG.step()));
      }
      if (averageValue < CONFIG.minTemperature()) {
        logger.info("Building[{}] Temperature is too low! Value: {}", building, averageValue);
        HTTPClient.getInstance()
            .sendCommand(regulatorId, regulatorLastResult.addAndGet(CONFIG.step()));
      }
    }

    private long getObservationTimestamp(Model model) {
      ResultSet rs = QueryExecutionFactory.create(QUERY_TIMESTAMP, model).execSelect();
      Long res = null;

      if (rs.hasNext()) {
        QuerySolution solution = rs.next();
        res = ZonedDateTime
            .parse(solution.getLiteral("?ts").getString(), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .toInstant()
            .toEpochMilli();
      }
      return res;
    }

    @Override
    public void onCompleted() {
      logger.error("Building[{}] Subscription cancelled!");
    }

    @Override
    public void onError(Throwable e) {
      logger.error("Building[{}] {}", building, e);
    }
  }

  private class RegulatorObserver extends MultiThreadRDFModelObserver {

    @Override
    public void onNext(Model message, long __) {
      regulatorLastResult.set(getValueFromModel(message, QUERY_VALUE));
    }

    @Override
    public void onCompleted() {
      logger.error("Building[{}] Subscription cancelled!");
    }

    @Override
    public void onError(Throwable e) {
      logger.error("Building[{}] {}", building, e);
    }
  }
}
