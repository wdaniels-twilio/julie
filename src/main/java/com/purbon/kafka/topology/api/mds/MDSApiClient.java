package com.purbon.kafka.topology.api.mds;

import static com.purbon.kafka.topology.api.mds.RequestScope.RESOURCE_NAME;
import static com.purbon.kafka.topology.api.mds.RequestScope.RESOURCE_PATTERN_TYPE;
import static com.purbon.kafka.topology.api.mds.RequestScope.RESOURCE_TYPE;

import com.purbon.kafka.topology.api.mds.http.HttpDeleteWithBody;
import com.purbon.kafka.topology.roles.AdminRoleRunner;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import com.purbon.kafka.topology.utils.JSON;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MDSApiClient {

  private static final Logger LOGGER = LogManager.getLogger(MDSApiClient.class);

  private final String mdsServer;
  private String basicCredentials;

  private AuthenticationCredentials authenticationCredentials;
  private String kafkaClusterID;
  private String schemaRegistryClusterID;
  private String connectClusterID;

  public static String KAFKA_CLUSTER_ID_LABEL = "kafka-cluster";
  public static String SCHEMA_REGISTRY_CLUSTER_ID_LABEL = "schema-registry-cluster";
  public static String CONNECT_CLUSTER_ID_LABEL = "connect-cluster";

  public MDSApiClient(String mdsServer) {
    this.mdsServer = mdsServer;
    this.kafkaClusterID = "";
    this.schemaRegistryClusterID = "";
  }

  public void login(String user, String password) {
    String userAndPassword = user + ":" + password;
    basicCredentials = Base64.getEncoder().encodeToString(userAndPassword.getBytes());
  }

  public AuthenticationCredentials getCredentials() {
    return authenticationCredentials;
  }

  public void authenticate() {
    HttpGet request = new HttpGet(mdsServer + "/security/1.0/authenticate");
    request.addHeader("accept", " application/json");
    request.addHeader("Authorization", "Basic " + basicCredentials);

    try {
      String responseAsString = get(request);
      if (!responseAsString.isEmpty()) {
        Map<String, Object> responseMap = JSON.toMap(responseAsString);

        authenticationCredentials =
            new AuthenticationCredentials(
                responseMap.get("auth_token").toString(),
                responseMap.get("token_type").toString(),
                Integer.valueOf(responseMap.get("expires_in").toString()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public AdminRoleRunner bind(String principal, String role) {
    return new AdminRoleRunner(principal, role, this);
  }

  public TopologyAclBinding bind(String principal, String role, String topic, String patternType) {
    return bind(principal, role, topic, "Topic", patternType);
  }

  public TopologyAclBinding bind(String principal, String role, RequestScope scope) {
    HttpPost postRequest =
        new HttpPost(
            mdsServer + "/security/1.0/principals/" + principal + "/roles/" + role + "/bindings");
    postRequest.addHeader("accept", " application/json");
    postRequest.addHeader("Content-Type", "application/json");
    postRequest.addHeader("Authorization", "Basic " + basicCredentials);

    try {
      postRequest.setEntity(new StringEntity(scope.asJson()));
      LOGGER.debug("bind.entity: " + scope.asJson());
      post(postRequest);
      ResourceType resourceType = ResourceType.fromString(scope.getResource(0).get(RESOURCE_TYPE));
      String resourceName = scope.getResource(0).get(RESOURCE_NAME);
      String patternType = scope.getResource(0).get(RESOURCE_PATTERN_TYPE);
      return new TopologyAclBinding(resourceType, resourceName, "*", role, principal, patternType);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public TopologyAclBinding bind(
      String principal, String role, String resource, String resourceType, String patternType) {

    RequestScope scope = new RequestScope();
    scope.setClusters(getKafkaClusterIds());
    scope.addResource(resourceType, resource, patternType);
    scope.build();

    return bind(principal, role, scope);
  }

  /**
   * Remove the role (cluster or resource scoped) from the principal at the given scope/cluster.
   * No-op if the user doesn’t have the role. Callable by Admins.
   *
   * @param principal Fully-qualified KafkaPrincipal string for a user or group.
   * @param role The name of the role.
   * @param scope The request scope
   */
  public void deleteRole(String principal, String role, RequestScope scope) {
    HttpDeleteWithBody request =
        new HttpDeleteWithBody(
            mdsServer + "/security/1.0/principals/" + principal + "/roles/" + role);
    request.addHeader("accept", " application/json");
    request.addHeader("Content-Type", "application/json");
    request.addHeader("Authorization", "Basic " + basicCredentials);
    LOGGER.debug("deleteRole: " + request.getURI());
    try {
      request.setEntity(new StringEntity(scope.asJson()));
      LOGGER.debug("bind.entity: " + scope.asJson());
      delete(request);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public TopologyAclBinding bindRole(
      String principal, String role, String resourceName, Map<String, Object> scope) {
    HttpPost postRequest =
        new HttpPost(mdsServer + "/security/1.0/principals/" + principal + "/roles/" + role);
    postRequest.addHeader("accept", " application/json");
    postRequest.addHeader("Content-Type", "application/json");
    postRequest.addHeader("Authorization", "Basic " + basicCredentials);

    try {
      postRequest.setEntity(new StringEntity(JSON.asString(scope)));
      LOGGER.debug("bind.entity: " + JSON.asString(scope));
      post(postRequest);
      return new TopologyAclBinding(
          ResourceType.CLUSTER, resourceName, "*", role, principal, PatternType.ANY.name());
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public List<String> lookupRoles(String principal) {
    HttpPost postRequest =
        new HttpPost(mdsServer + "/security/1.0/lookup/principals/" + principal + "/roleNames");
    postRequest.addHeader("accept", " application/json");
    postRequest.addHeader("Content-Type", "application/json");
    postRequest.addHeader("Authorization", "Basic " + basicCredentials);

    List<String> roles = new ArrayList<>();

    try {
      postRequest.setEntity(new StringEntity(JSON.asString(getKafkaClusterIds())));
      String stringResponse = post(postRequest);
      if (!stringResponse.isEmpty()) {
        roles = JSON.toArray(stringResponse);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return roles;
  }

  public Map<String, Map<String, String>> getKafkaClusterIds() {
    HashMap<String, String> clusterIds = new HashMap<>();
    if (!kafkaClusterID.isEmpty()) clusterIds.put(KAFKA_CLUSTER_ID_LABEL, kafkaClusterID);

    Map<String, Map<String, String>> clusters = new HashMap<>();
    clusters.put("clusters", clusterIds);
    return clusters;
  }

  public Map<String, Map<String, String>> getClusterIds() {
    HashMap<String, String> clusterIds = new HashMap<>();
    if (!kafkaClusterID.isEmpty()) clusterIds.put(KAFKA_CLUSTER_ID_LABEL, kafkaClusterID);
    if (!schemaRegistryClusterID.isEmpty())
      clusterIds.put(SCHEMA_REGISTRY_CLUSTER_ID_LABEL, schemaRegistryClusterID);
    if (!connectClusterID.isEmpty()) clusterIds.put(CONNECT_CLUSTER_ID_LABEL, connectClusterID);

    // clusterIds.put("connect-cluster", "connect-cluster");
    // clusterIds.put("ksql-cluster", "ksqlCluster");

    Map<String, Map<String, String>> clusters = new HashMap<>();
    clusters.put("clusters", clusterIds);
    return clusters;
  }

  private final CloseableHttpClient httpClient = HttpClients.createDefault();

  private String get(HttpGet request) throws IOException {
    LOGGER.debug("GET.request: " + request);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      LOGGER.debug("GET.response: " + response);
      HttpEntity entity = response.getEntity();
      // Header headers = entity.getContentType();
      String result = "";
      if (entity != null) {
        result = EntityUtils.toString(entity);
      }

      return result;
    }
  }

  private String post(HttpPost request) throws IOException {
    LOGGER.debug("POST.request: " + request);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      LOGGER.debug("POST.response: " + response);
      HttpEntity entity = response.getEntity();
      // Header headers = entity.getContentType();
      String result = "";
      if (entity != null) {
        result = EntityUtils.toString(entity);
      }

      return result;
    }
  }

  private String delete(HttpDeleteWithBody request) throws IOException {
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

  public void setKafkaClusterId(String kafkaClusterID) {
    this.kafkaClusterID = kafkaClusterID;
  }

  public void setSchemaRegistryClusterID(String schemaRegistryClusterID) {
    this.schemaRegistryClusterID = schemaRegistryClusterID;
  }

  public void setConnectClusterID(String connectClusterID) {
    this.connectClusterID = connectClusterID;
  }
}