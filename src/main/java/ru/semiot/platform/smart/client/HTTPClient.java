/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.semiot.platform.smart.client;

import java.io.IOException;
import java.io.StringReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateException;
import javax.security.cert.X509Certificate;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
import org.apache.http.util.EntityUtils;
import org.apache.jena.query.QueryExecutionFactory;
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
public class HTTPClient {

  private static final Logger logger = LoggerFactory.getLogger(HTTPClient.class);
  private static final HTTPClient INSTANCE = new HTTPClient();
  private String url, username, password, cookie;
  private CloseableHttpClient httpclient;
  private static final String QUERY_SYSTEMS = "SELECT ?system_id { "
      + "?z a <${HOST}/doc#${TYPE}>; "
      + "<http://purl.org/dc/terms/identifier> ?system_id "
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

    }
  }

  public void sendCommand(String system_id, double value) {
    String command_url = url + "/systems/" + system_id + "/processes/pressure";
    logger.debug("Try to send command to url '{}' with value {}", command_url, value);
    HttpPost httpPost = new HttpPost(command_url);
    String entity = COMMAND.replace("${HOST}", this.url).replace("${SYSTEM_ID}", system_id).replace("${VALUE}", Double.toString(value));
    long stop;
    try {
      httpPost.setEntity(new StringEntity(entity));
      httpPost.setHeader("Content-Type", "text/turtle");
      httpPost.setHeader("Cookie", cookie);
      long st = System.currentTimeMillis();
      CloseableHttpResponse response = httpclient.execute(httpPost);
      stop = System.currentTimeMillis();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        logger.debug("Command to url '{}' is sent successfuly!", command_url);
        logger.info("Command is executed by {} ms", stop-st);
      } else {
        logger.warn("Command failed by {} ms", stop-st);
      }
      response.close();
    } catch (IOException ex) {
      logger.warn("Catch exception! Message is {}", ex.getMessage(), ex);
    }
  }

  public HashMap<String, HashMap<String, String>> getRegulators() {
    return sendQuery(false);
  }

  public HashMap<String, HashMap<String, String>> getDevices() {
    return sendQuery(true);
  }

  private HashMap<String, HashMap<String, String>> sendQuery(boolean isDevice) {
    HashMap<String, HashMap<String, String>> map = new HashMap<>();
    HttpGet httpGet = new HttpGet(url + "/systems/");
    String type = isDevice ? "TemperatureDevice" : "Regulator";
    try {
      httpGet.setHeader("Accept", "application/ld+json");
      httpGet.setHeader("Cookie", cookie);
      CloseableHttpResponse response = httpclient.execute(httpGet);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn("Something went wrong! Response code is {}, reason {}",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
        response.close();
      } else {
        Model model = ModelFactory.createDefaultModel();
        model.read(response.getEntity().getContent(), null, RDFLanguages.strLangJSONLD);
        ResultSet rs = QueryExecutionFactory.create(
            QUERY_SYSTEMS.replace("${HOST}", url).replace("${TYPE}", type), model)
            .execSelect();
        response.close();
        while (rs.hasNext()) {
          QuerySolution solution = rs.next();
          String system_id = solution.getLiteral("?system_id").getString();
          String topic = isDevice ? system_id + ".observations." + system_id + "-temperature"
              : system_id + ".commandresults.pressure";
          logger.debug("Try to get building for system {}", system_id);
          String building = getBuilding(system_id);
          if (map.containsKey(building)) {
            map.get(building).put(system_id, topic);
          } else {
            HashMap<String, String> m = new HashMap<>();
            m.put(system_id, topic);
            map.put(building, m);
          }
        }

      }

    } catch (IOException ex) {
      logger.warn("Can't get {}! Message is {}", type, ex.getMessage(), ex);
    }
    return map;
  }

  private String getBuilding(String system_id) {
    HttpGet httpGet = new HttpGet(url + "/systems/" + system_id);
    try {
      httpGet.setHeader("Accept", "application/ld+json");
      httpGet.setHeader("Cookie", cookie);
      CloseableHttpResponse response = httpclient.execute(httpGet);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn("Something went wrong! Response code is {}, reason {}",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
        response.close();
      } else {
        Model model = ModelFactory.createDefaultModel();
        model.read(response.getEntity().getContent(), null, RDFLanguages.strLangJSONLD);
        response.close();
        ResultSet rs = QueryExecutionFactory.create(QUERY_PLACE, model).execSelect();
        while (rs.hasNext()) {
          QuerySolution solution = rs.next();
          return solution.getLiteral("?building").getString();
        }

      }

    } catch (IOException ex) {
      logger.warn("Can't get building for id {}! Message is {}", system_id, ex.getMessage(), ex);
    }
    return null;
  }

  public double getLastCommandResult(String regulator_id) {
    String uri = url + "/systems/" + regulator_id + "/processes/pressure/commandResults";
    HttpGet httpGet = new HttpGet(uri);
    Double d = null;
    try {
      httpGet.setHeader("Accept", "application/ld+json");
      httpGet.setHeader("Cookie", cookie);
      CloseableHttpResponse response = httpclient.execute(httpGet);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        logger.warn("Something went wrong! Response code is {}, reason {}",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase());
        response.close();
      } else {
        Model model = ModelFactory.createDefaultModel();
        String desc = EntityUtils.toString(response.getEntity());
        model.read(new StringReader(desc), null, RDFLanguages.strLangJSONLD);
        //model.read(response.getEntity().getContent(), null, RDFLanguages.strLangJSONLD);
        response.close();
        ResultSet rs = QueryExecutionFactory.create(QUERY_COMMAND_RESULT.replace("${URI}", uri), model).execSelect();
        while (rs.hasNext()) {
          QuerySolution solution = rs.next();
          return Double.parseDouble(solution.getLiteral("?value").getString().replace(',', '.'));
        }

      }

    } catch (IOException ex) {
      logger.warn("Can't get commandResult for id {}! Message is {}", regulator_id, ex.getMessage(), ex);
    }
    return d;
  }
}
