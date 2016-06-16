package ru.semiot.platform.smart.client;

import java.io.StringReader;
import java.util.HashMap;
import org.apache.jena.atlas.web.auth.HttpAuthenticator;
import org.apache.jena.atlas.web.auth.SimpleAuthenticator;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
public class SPARQLClient {

  private static final Logger logger = LoggerFactory.getLogger(SPARQLClient.class);
  private static final SPARQLClient INSTANCE = new SPARQLClient();
  private HttpAuthenticator authenticator;
  private String url;
  private static final String URL_TEMPERATURE_DEVICE_PROTOTYPE
      = "https://raw.githubusercontent.com/semiotproject/semiot-drivers/master/temperature-simulator/"
      + "src/main/resources/ru/semiot/drivers/temperature/simulator/prototype.ttl#TemperatureDevice";
  private static final String URL_TEMPERATURE_REGULATOR_PROTOTYPE
      = "https://raw.githubusercontent.com/semiotproject/semiot-drivers/master/regulator-simulator/"
      + "src/main/resources/ru/semiot/drivers/regulator/simulator/prototype.ttl#Regulator";

  private static final String QUERY = "SELECT ?system_id ?topic ?building{ "
      + " ?x <http://purl.org/dc/terms/identifier> ?system_id; "
      + "   <http://w3id.org/semiot/ontologies/proto#hasPrototype> <${PROTOTYPE}> ;"
      + "   <http://www.w3.org/2003/01/geo/wgs84_pos#location> [ "
      + "     a <http://schema.org/Place>;"
      + "     <http://schema.org/branchCode> ?building"
      + "   ]"
      + " GRAPH <urn:semiot:graphs:private> { "
      + "   ?x <http://purl.org/NET/ssnext/communication#hasCommunicationEndpoint> [ "
      + "     <http://purl.org/NET/ssnext/communication#protocol> \"WAMP\"; "
      + "     <http://purl.org/NET/ssnext/communication#provide> \"${TYPE}\"; "
      + "     <http://purl.org/NET/ssnext/communication#topic> ?topic "
      + "	]"
      + " }"
      + "}";

  private static final String QUERY_VALUE = "SELECT ?value { "
      + "?z a <http://w3id.org/semiot/ontologies/semiot#MappingParameter>; "
      + "<http://www.loa-cnr.it/ontologies/DUL.owl#hasParameterDataValue> ?value "
      + "}";

  private static final String QUERY_OBS_VALUE = "SELECT ?value { "
      + "?z a <http://qudt.org/schema/qudt#QuantityValue>; "
      + "<http://qudt.org/schema/qudt#quantityValue> ?value "
      + "}";

  private SPARQLClient() {
  }

  public static SPARQLClient getInstance() {
    return INSTANCE;
  }

  public void init(String username, String password, String url) {
    authenticator = new SimpleAuthenticator(username, password.toCharArray());
    this.url = url;
  }

  public HashMap<String, HashMap<String, String>> getRegulators() {
    return execSelect(QUERY.replace("${PROTOTYPE}", URL_TEMPERATURE_REGULATOR_PROTOTYPE).replace("${TYPE}", "commandresults"), false);
  }

  public HashMap<String, HashMap<String, String>> getDevices() {
    return execSelect(QUERY.replace("${PROTOTYPE}", URL_TEMPERATURE_DEVICE_PROTOTYPE).replace("${TYPE}", "observations"), true);
  }

  private HashMap<String, HashMap<String, String>> execSelect(String query, boolean isDevice) {
    HashMap<String, HashMap<String, String>> map = new HashMap<>();
    ResultSet rs = QueryExecutionFactory.createServiceRequest(url,
        QueryFactory.create(query), authenticator).execSelect();
    while (rs.hasNext()) {
      QuerySolution solution = rs.next();
      String system = solution.getLiteral("?system_id").getString();
      String topic = solution.getLiteral("?topic").getString();
      //WTF?!?!?
      if (isDevice) {
        topic += "." + system + "-temperature";
      } else {
        topic += ".pressure";
      }

      String building = solution.getLiteral("?building").getString();
      if (map.containsKey(building)) {
        map.get(building).put(system, topic);
      } else {
        HashMap<String, String> m = new HashMap<>();
        m.put(system, topic);
        map.put(building, m);
      }
    }
    return map;
  }

  public double getValueFromCommandResult(String commandResult) {
    //logger.debug(commandResult);
    Model model = ModelFactory.createDefaultModel();
    model.read(new StringReader(commandResult), null, RDFLanguages.strLangJSONLD);
    ResultSet rs = QueryExecutionFactory.create(QUERY_VALUE, model).execSelect();
    Double value = null;
    while (rs.hasNext()) {
      QuerySolution solution = rs.next();
      value = Double.parseDouble(solution.getLiteral("?value").getString().replace(',', '.'));
    }
    return value;
  }

  public double getValueFromObservation(String observation) {
    //logger.debug(observation);
    Model model = ModelFactory.createDefaultModel();
    model.read(new StringReader(observation), null, RDFLanguages.strLangJSONLD);
    ResultSet rs = QueryExecutionFactory.create(QUERY_OBS_VALUE, model).execSelect();
    Double value = null;
    while (rs.hasNext()) {
      QuerySolution solution = rs.next();
      String v = solution.getLiteral("?value").getString();
      logger.info("Value is {}", v);
      value = Double.parseDouble(v.replace(',', '.'));
    }
    return value;
  }
}