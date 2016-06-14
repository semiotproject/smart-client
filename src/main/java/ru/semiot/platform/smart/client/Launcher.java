package ru.semiot.platform.smart.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateException;
import javax.security.cert.X509Certificate;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
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
  private String cookie;
  private String COMMAND_URL;
  CloseableHttpClient httpclient;

  private static final String COMMAND
      = "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
      + "@prefix dul: <http://www.loa-cnr.it/ontologies/DUL.owl#> .\n"
      + "@prefix dcterms: <http://purl.org/dc/terms/> .\n"
      + "@prefix semiot: <http://w3id.org/semiot/ontologies/semiot#> .\n"
      + "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
      + "@prefix : <https://raw.githubusercontent.com/semiotproject/semiot-drivers/master/regulator-simulator/src/main/resources/ru/semiot/drivers/regulator/simulator/prototype.ttl#> .\n"
      + "\n"
      + "[ a semiot:ChangeValueCommand ;\n"
      + "  dcterms:identifier \"change-regulator_value\" ;\n"
      + "  semiot:forProcess <https://demo.semiot.ru/systems/${SYSTEM_ID}/processes/pressure> ;\n"
      + "  dul:associatedWith <https://demo.semiot.ru/systems/${SYSTEM_ID}> ;\n"
      + "  dul:hasParameter [\n"
      + "    a semiot:MappingParameter ;\n"
      + "    semiot:forParameter :Regulator-ChangeValue-Pressure ;\n"
      + "    dul:hasParameterDataValue \"${VALUE}\"^^xsd:double ;\n"
      + "  ] ;\n"
      + "] .";

  public void run() {
    devicesInBuildings = SPARQLClient.getInstance().getDevices();
    regulatorsInBuildings = SPARQLClient.getInstance().getRegulators();

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
                int start = commandResult.indexOf("dul:hasParameterDataValue\":") + "dul:hasParameterDataValue\":".length();
                double v = Double.parseDouble(commandResult.substring(start, commandResult.indexOf("}", start)));

                //double v = SPARQLClient.getInstance().getValueFromCommandResult(commandResult);
                regulatorsLastResults.put(regulator, v);
              }
            });
        sendCommand(regulator, 100);
      }
    }

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
                int start = observation.indexOf("qudt:quantityValue\":") + "qudt:quantityValue\":".length();
                double v = Double.parseDouble(observation.substring(start, observation.indexOf("}", start)));

                //double v = SPARQLClient.getInstance().getValueFromObservation(observation);
                appendValue(device, v);
              }
            });
      }
    }
    while (true) {

    }

  }

  public synchronized void appendValue(String device, double value) {
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

  public void checkTemperature(String building) {
    double avg = sumTempInBuildings.get(building) / devicesInBuildings.get(building).size();
    logger.debug("Check temperature in building {}. Average temperature is {}", building, avg);
    if (avg > CONFIG.maxTemperature()) {
      logger.debug("Temperature is big!");
      for (String regulator : regulatorsInBuildings.get(building).keySet()) {
        sendCommand(regulator, regulatorsLastResults.get(regulator) - CONFIG.step());
      }
    }
    if (avg < CONFIG.minTemperature()) {
      logger.debug("Temperature is small!");
      for (String regulator : regulatorsInBuildings.get(building).keySet()) {
        sendCommand(regulator, regulatorsLastResults.get(regulator) + CONFIG.step());
      }
    }
  }

  public Launcher() {
    logger.debug("Try to initialize launcher");
    SPARQLClient.getInstance().init(CONFIG.sparqlUsername(),
        CONFIG.sparqlPassword(),
        CONFIG.sparqlUrl());
    try {
      httpclient = (CloseableHttpClient) createHttpClient_AcceptsUntrustedCerts();
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException ex) {
      logger.error("Can't init httpClient", ex);
    }
    cookie = getCookie();
    COMMAND_URL = CONFIG.hostUrl() + "/systems/${SYSTEM_ID}/processes/pressure";
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

  private final HttpClient createHttpClient_AcceptsUntrustedCerts() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    HttpClientBuilder b = HttpClientBuilder.create();

    SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
      public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        return true;
      }

      @Override
      public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1) throws java.security.cert.CertificateException {
        return true;
      }
    }).build();
    b.setSslcontext(sslContext);

    HostnameVerifier hostnameVerifier = SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;
    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
    Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .register("https", sslSocketFactory)
        .build();

    PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
    b.setConnectionManager(connMgr);

    HttpClient client = b.build();
    return client;
  }

  private String getCookie() {
    logger.debug("Try to authorize and get cookie");
    HttpPost httpPost = new HttpPost(CONFIG.hostUrl() + "/auth");
    String cookie = "";
    String entity = "{\"username\": \"${USERNAME}\", \"password\": \"${PASSWORD}\" }"
        .replace("${USERNAME}", CONFIG.hostUsername())
        .replace("${PASSWORD}", CONFIG.hostPassword());
    try {
      httpPost.setEntity(new StringEntity(entity));
      logger.debug("Try to send POST query");
      CloseableHttpResponse response = httpclient.execute(httpPost);
      cookie = response.getFirstHeader("Set-Cookie").getValue();
      logger.debug("Cookie are got!");
      response.close();
    } catch (IOException ex) {

    }
    return cookie;
  }

  private void sendCommand(String system_id, double value) {
    String url = COMMAND_URL.replace("${SYSTEM_ID}", system_id);
    logger.debug("Try to send command to url '{}' with value {}", url, value);
    HttpPost httpPost = new HttpPost(url);
    String entity = COMMAND.replace("${SYSTEM_ID}", system_id).replace("${VALUE}", Double.toString(value));
    try {
      httpPost.setEntity(new StringEntity(entity));
      httpPost.setHeader("Content-Type", "text/turtle");
      httpPost.setHeader("Cookie", cookie);
      CloseableHttpResponse response = httpclient.execute(httpPost);
      if (response.getStatusLine().getStatusCode() == 200) {
        logger.debug("Command is sent successfuly!");
      } else {
        logger.warn("Something went wrong! Response code is {}, reason {}",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
      }
      response.close();
    } catch (IOException ex) {
      logger.warn("Cath exception! Message is {}", ex.getMessage(), ex);
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
}
