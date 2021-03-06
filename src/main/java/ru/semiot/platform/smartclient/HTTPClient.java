/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.semiot.platform.smartclient;

import org.aeonbits.owner.ConfigFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
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
import org.apache.http.util.EntityUtils;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateException;
import javax.security.cert.X509Certificate;

/**
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
public class HTTPClient {

  private static final Logger logger = LoggerFactory.getLogger(HTTPClient.class);
  private static final ClientConfig CONFIG = ConfigFactory.create(ClientConfig.class);
  private static final HTTPClient INSTANCE = new HTTPClient();
  private String url, username, password, cookie;
  private CloseableHttpClient httpclient;
  private static final Scheduler SCHEDULER = Schedulers.from(Executors.newFixedThreadPool(10));

  private static final String QUERY_TOTAL_ITEMS = "SELECT ?count {"
      + "?x <http://www.w3.org/ns/hydra/core#totalItems> ?count}";

  private static final String QUERY_SYSTEMS = "SELECT ?system_id ?type { "
      + "?z a ?type; "
      + "<http://purl.org/dc/terms/identifier> ?system_id . "
      + "{ ?z a <${HOST}/doc#Regulator> }"
      + "UNION { ?z a <${HOST}/doc#TemperatureDevice> }"
      + "}";

  private static final String QUERY_COMMAND_RESULT = "SELECT ?value { "
      + "?z a <http://w3id.org/semiot/ontologies/semiot#CommandResult>; "
      + "<http://w3id.org/semiot/ontologies/semiot#isResultOf> ["
      + "<http://www.loa-cnr.it/ontologies/DUL.owl#hasParameter>/<http://www.loa-cnr.it/ontologies/DUL.owl#hasParameterDataValue> ?value "
      + "]"
      + "}";

  private static final String QUERY_PLACE = "SELECT ?building { "
      + "?z a <http://schema.org/Place>; "
      + "<http://schema.org/branchCode> ?building "
      + "}";

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
      + "  semiot:forProcess <${HOST}/systems/${SYSTEM_ID}/processes/pressure> ;\n"
      + "  dul:associatedWith <${HOST}/systems/${SYSTEM_ID}> ;\n"
      + "  dul:hasParameter [\n"
      + "    a semiot:MappingParameter ;\n"
      + "    semiot:forParameter :Regulator-ChangeValue-Pressure ;\n"
      + "    dul:hasParameterDataValue \"${VALUE}\"^^xsd:double ;\n"
      + "  ] ;\n"
      + "] .";
  private static String typeTemperatureDevice = "${HOST}/doc#TemperatureDevice";


  public void init(String hosturl, String pass, String user) {
    this.url = hosturl;
    this.username = user;
    this.password = pass;
    try {
      httpclient = (CloseableHttpClient) createHttpClient_AcceptsUntrustedCerts();
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException ex) {
      logger.error("Can't init httpClient", ex);
    }
    setCookie();
  }

  public static HTTPClient getInstance() {
    return INSTANCE;
  }

  private HttpClient createHttpClient_AcceptsUntrustedCerts() throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    HttpClientBuilder b = HttpClientBuilder.create();

    SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
      public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        return true;
      }

      @Override
      public boolean isTrusted(java.security.cert.X509Certificate[] arg0, String arg1)
          throws java.security.cert.CertificateException {
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
    connMgr.setDefaultMaxPerRoute(10);
    b.setConnectionManager(connMgr);

    HttpClient client = b.build();
    return client;
  }

  private void setCookie() {
    //logger.debug("Try to authorize and get cookie");
    HttpPost httpPost = new HttpPost(url + "/auth");
    String entity = "{\"username\": \"${USERNAME}\", \"password\": \"${PASSWORD}\" }"
        .replace("${USERNAME}", username)
        .replace("${PASSWORD}", password);
    try {
      httpPost.setEntity(new StringEntity(entity));
      //logger.debug("Try to send POST query");
      CloseableHttpResponse response = httpclient.execute(httpPost);
      cookie = response.getFirstHeader("Set-Cookie").getValue();
      //logger.debug("Cookie are got!");
      response.close();
    } catch (IOException ex) {
      logger.warn(ex.getMessage(), ex);
    }
  }

  public void sendCommand(String systemId, double value) {
    String command_url = url + "/systems/" + systemId + "/processes/pressure";
    logger.debug("Try to send command to url '{}' with value {}", command_url, value);

    String entity = COMMAND.replace("${HOST}", this.url)
        .replace("${SYSTEM_ID}", systemId)
        .replace("${VALUE}", Double.toString(value));

    HttpPost httpPost;
    try {
      httpPost = newHttpPost(command_url, new StringEntity(entity));
    } catch (UnsupportedEncodingException ex) {
      logger.error(ex.getMessage(), ex);

      throw new IllegalStateException();
    }

    long start = System.currentTimeMillis();

    try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
      long stop = System.currentTimeMillis();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        logger.debug("Command to url '{}' is sent successfuly!", command_url);
        logger.info("Command executed for {} ms", stop - start);
      } else {
        logger.warn("Command failed for {} ms", stop - start);
      }
    } catch (Throwable ex) {
      logger.warn("Catch exception! Message is {}", ex.getMessage(), ex);
    }
  }

  public void getSensorsAndRegulators(Map<String, Map<String, String>> sensors,
      Map<String, Map<String, String>> regulators) {
    String typeTemperDevice = typeTemperatureDevice.replace("${HOST}", url);

    int count = getPage(1, typeTemperDevice, sensors, regulators);
    int countPage = (int) Math.ceil((double) count / CONFIG.sizePage());

    logger.debug("Count page = {}", countPage);

    for (int i = 2; i <= countPage; i++) {
      getPage(i, typeTemperDevice, sensors, regulators);
    }

    printlnMap(sensors);
    printlnMap(regulators);
  }

  private boolean printlnMap(Map<String, Map<String, String>> map) {
    int count = 0;
    for (Entry<String, Map<String, String>> entry : map.entrySet()) {
      String key = entry.getKey();
      Map<String, String> value = entry.getValue();
      for (Entry<String, String> entry1 : value.entrySet()) {
        ++count;
        // logger.debug("{} {}", ++count, entry1.getValue());
      }
    }
    logger.debug("Added to map {}", count);

    return false;
  }

  private int getPage(int page, String typeTemperDevice, Map<String, Map<String, String>> devices,
      Map<String, Map<String, String>> regulators) {
    long startTimestamp = System.currentTimeMillis();
    List<Observable<Device>> obsers = new ArrayList<>();

    HttpGet httpGet;
    try {
      URIBuilder uriBuilder = new URIBuilder(url);
      uriBuilder.setPath("/systems").setParameter("page", String.valueOf(page)).setParameter("size",
          String.valueOf(CONFIG.sizePage()));
      httpGet = newHttpGet(uriBuilder.build().toASCIIString());
    } catch (Throwable ex) {
      throw new IllegalStateException();
    }

    try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn("Something went wrong! Response code is {}, reason {}",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
      } else {
        Model model = ModelFactory.createDefaultModel();
        model.read(response.getEntity().getContent(), null, RDFLanguages.strLangJSONLD);
        ResultSet rs =
            QueryExecutionFactory.create(QUERY_SYSTEMS.replace("${HOST}", url), model).execSelect();

        while (rs.hasNext()) {
          QuerySolution solution = rs.next();
          String system_id = solution.getLiteral("?system_id").getString();
          String type = solution.getResource("?type").getURI();
          obsers.add(getBuldingAsync(system_id, type));
        }
        Iterator<Device> iterator = Observable.merge(obsers).toBlocking().toIterable().iterator();
        while (iterator.hasNext()) {
          Device d = iterator.next();
          String system_id = d.getSystemId();
          String type = d.getType();
          String topic = type.equals(typeTemperDevice) ? system_id + ".observations." + system_id + "-temperature"
              : system_id + ".commandresults.pressure";
          addSystemToMap(type.equals(typeTemperDevice) ? devices : regulators, d.getBuilding(), d.getSystemId(), topic);
        }
        if (page == 1) {
          ResultSet rs1 = QueryExecutionFactory.create(QUERY_TOTAL_ITEMS, model).execSelect();
          while (rs1.hasNext()) {
            logger.info("Page {} processed for {} ms", page, System.currentTimeMillis() - startTimestamp);
            return rs1.next().getLiteral("count").getInt();
          }
        }
      }
    } catch (Throwable e) {
      logger.error(e.getMessage(), e);
    }
    logger.info("Page {} processed for {} ms", page, System.currentTimeMillis() - startTimestamp);
    return 0;
  }

  private void addSystemToMap(Map<String, Map<String, String>> map, String building,
      String system_id, String topic) {

    if (map.containsKey(building)) {
      map.get(building).put(system_id, topic);
    } else {
      HashMap<String, String> m = new HashMap<>();
      m.put(system_id, topic);
      map.put(building, m);
    }
  }

  private Observable<Device> getBuldingAsync(String system_id, String topic) {
    return Observable.create(o -> {
      try {
        String building = getBuilding(system_id);

        Device device = new Device(building, system_id, topic);
        o.onNext(device);
      } catch (Exception e) {
        o.onError(e);
      }
      o.onCompleted();
    }).subscribeOn(SCHEDULER).cast(Device.class);
  }

  private String getBuilding(String system_id) {
    logger.debug("Try to get building for system {}", system_id);

    HttpGet httpGet = newHttpGet(url + "/systems/" + system_id);

    try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn("Something went wrong! Response code is {}, reason {}",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
      } else {
        Model model = ModelFactory.createDefaultModel();
        model.read(response.getEntity().getContent(), null, RDFLanguages.strLangJSONLD);
        ResultSet rs = QueryExecutionFactory.create(QUERY_PLACE, model).execSelect();
        while (rs.hasNext()) {
          QuerySolution solution = rs.next();
          return solution.getLiteral("?building").getString();
        }
      }
    } catch (Throwable ex) {
      logger.warn("Can't get building for id {}! Message is {}", system_id, ex.getMessage(), ex);
    }
    return null;
  }

  public double getLastCommandResult(String regulatorId) {
    String uri = url + "/systems/" + regulatorId + "/processes/pressure/commandResults";
    HttpGet httpGet = newHttpGet(uri);

    try (CloseableHttpResponse response = httpclient.execute(httpGet)) {
      if (response != null) {
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
          logger.warn("Something went wrong! Response code is {}, reason {}",
              response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
        } else {
          Model model = ModelFactory.createDefaultModel();
          String desc = EntityUtils.toString(response.getEntity());
          model.read(new StringReader(desc), null, RDFLanguages.strLangJSONLD);

          ResultSet rs = QueryExecutionFactory
              .create(QUERY_COMMAND_RESULT.replace("${URI}", uri), model)
              .execSelect();
          if (rs.hasNext()) {
            QuerySolution solution = rs.next();
            return Double.parseDouble(solution.getLiteral("?value").getString().replace(',', '.'));
          } else {
            logger.warn("Can't find last command result at {}",
                httpGet.getURI().toASCIIString());
            throw new IllegalStateException();
          }
        }
      } else {
        logger.error("Response for the last command result is null! URL: {}",
            httpGet.getURI().toASCIIString());
      }
    } catch (IOException ex) {
      logger.warn("Can't get commandResult for [{}]! Message: {}", regulatorId, ex.getMessage(), ex);
    }
    throw new IllegalStateException();
  }

  private HttpGet newHttpGet(String url) {
    HttpGet request = new HttpGet(url);
    request.setHeader("Accept", "application/ld+json");
    request.setHeader("Cookie", cookie);

    return request;
  }

  private HttpPost newHttpPost(String url, HttpEntity entity) {
    HttpPost request = new HttpPost(url);
    request.setEntity(entity);
    request.setHeader("Content-Type", "text/turtle");
    request.setHeader("Cookie", cookie);
    return request;
  }
}
