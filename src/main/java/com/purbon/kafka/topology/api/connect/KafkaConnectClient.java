package com.purbon.kafka.topology.api.connect;

import com.purbon.kafka.topology.client.http.HttpClient;
import com.purbon.kafka.topology.client.http.model.Response;
import java.io.IOException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaConnectClient {

  private static final Logger LOGGER = LogManager.getLogger(KafkaConnectClient.class);
  private final String connectServer;
  private final HttpClient httpClient;

  public KafkaConnectClient(String connectServer) {
    this.connectServer = connectServer;
    this.httpClient = new HttpClient();
  }

  public String createConnector(String connectorAsJson) throws IOException {
    HttpPost postRequest = new HttpPost(connectServer + "/connectors");
    postRequest.addHeader("accept", " application/json");
    postRequest.addHeader("Content-Type", "application/json");
    try {
      postRequest.setEntity(new StringEntity(connectorAsJson));
      LOGGER.debug(String.format("CreateConnector with body %s", connectorAsJson));
      return httpClient.post(postRequest);
    } catch (IOException e) {
      LOGGER.error("Something happened when creating a connector", e);
      throw new IOException(e);
    }
  }

  public void deleteConnector(String connector) throws IOException {
    HttpDelete deleteRequest = new HttpDelete(connector + "/connectors/" + connector);
    try {
      httpClient.delete(deleteRequest);
    } catch (IOException e) {
      LOGGER.error("Something happened when deleting connector " + connector, e);
      throw new IOException(e);
    }
  }

  public String getConnector(String connector) throws IOException {
    HttpGet request = new HttpGet(connectServer + "/connectors");
    request.addHeader("accept", " application/json");
    request.addHeader("Content-Type", "application/json");

    try {
      Response response = httpClient.get(request);
      return response.getResponseAsString();
    } catch (IOException e) {
      LOGGER.error("Something happened when getting the connector " + connector, e);
      throw new IOException(e);
    }
  }
}
