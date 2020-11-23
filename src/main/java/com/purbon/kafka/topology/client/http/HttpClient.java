package com.purbon.kafka.topology.client.http;

import com.purbon.kafka.topology.client.http.model.Response;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpClient {

  private static final Logger LOGGER = LogManager.getLogger(HttpClient.class);

  private CloseableHttpClient httpClient;

  public HttpClient() {
    httpClient = HttpClients.createDefault();
  }

  public Response get(HttpGet request) throws IOException {
    LOGGER.debug("GET.request: " + request);
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      LOGGER.debug("GET.response: " + response);
      return new Response(response);
    }
  }

  public String post(HttpPost request) throws IOException {
    LOGGER.debug("POST.request: " + request);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      LOGGER.debug("POST.response: " + response);
      HttpEntity entity = response.getEntity();
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode < 200 || statusCode > 299) {
        throw new IOException(
            "Something happened with the connection, response status code: "
                + statusCode
                + " "
                + request);
      }
      String result = "";
      if (entity != null) {
        result = EntityUtils.toString(entity);
      }
      return result;
    } catch (IOException ex) {
      LOGGER.error(ex);
      throw ex;
    }
  }

  public String delete(HttpRequestBase request) throws IOException {
    LOGGER.debug("DELETE.request: " + request);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      LOGGER.debug("DELETE.response: " + response);
      HttpEntity entity = response.getEntity();
      // Header headers = entity.getContentType();
      String result = "";
      if (entity != null) {
        result = EntityUtils.toString(entity);
      }

      return result;
    }
  }
}
