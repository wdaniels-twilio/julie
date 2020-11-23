package com.purbon.kafka.topology.model.users.connector;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.topology.model.DynamicUser;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConnectorAccount extends DynamicUser {

  private static final String DEFAULT_CONNECT_STATUS_TOPIC = "connect-status";
  private static final String DEFAULT_CONNECT_OFFSET_TOPIC = "connect-offsets";
  private static final String DEFAULT_CONNECT_CONFIGS_TOPIC = "connect-configs";
  private static final String DEFAULT_CONNECT_GROUP = "connect-cluster";

  private static final String DEFAULT_CONNECT_CLUSTER_ID = "connect-cluster";

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> status_topic;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> offset_topic;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> configs_topic;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> group;

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> cluster_id;

  private Optional<List<String>> connectors;

  public ConnectorAccount() {
    this("");
  }

  public ConnectorAccount(String principal) {
    this(
        principal,
        new HashMap<>(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public ConnectorAccount(JsonNode node) {
    this(node.get("principal").asText());
    parseConnectorProps(node);
  }

  private void parseConnectorProps(JsonNode node) {
    if (node.has("topics")) {
      JsonNode topicsNode = node.get("topics");
      HashMap<String, List<String>> topicsMap = new HashMap<>();
      Arrays.asList("read", "write")
          .forEach(
              key -> {
                if (topicsNode.has(key)) {
                  topicsMap.put(key, parseListOfStrings(topicsNode.get(key).elements()));
                }
              });
      setTopics(topicsMap);
    }

    status_topic =
        node.has("status_topic")
            ? Optional.of(node.get("status_topic").asText())
            : Optional.empty();
    offset_topic =
        node.has("offset_topic")
            ? Optional.of(node.get("offset_topic").asText())
            : Optional.empty();
    configs_topic =
        node.has("configs_topic")
            ? Optional.of(node.get("configs_topic").asText())
            : Optional.empty();
    group = node.has("group") ? Optional.of(node.get("group").asText()) : Optional.empty();
    cluster_id =
        node.has("cluster_id") ? Optional.of(node.get("cluster_id").asText()) : Optional.empty();
    if (node.has("connectors")) {
      connectors = Optional.of(parseListOfStrings(node.get("connectors").elements()));
    }
  }

  private List<String> parseListOfStrings(Iterator<JsonNode> it) {
    Iterable<JsonNode> iterable = () -> it;
    return StreamSupport.stream(iterable.spliterator(), false)
        .map(JsonNode::asText)
        .collect(Collectors.toList());
  }

  public ConnectorAccount(
      String principal,
      HashMap<String, List<String>> topics,
      Optional<String> status_topic,
      Optional<String> offset_topic,
      Optional<String> configs_topic,
      Optional<String> group,
      Optional<String> cluster_id,
      Optional<List<String>> connectors) {

    super(principal, topics);

    this.configs_topic = configs_topic;
    this.status_topic = status_topic;
    this.offset_topic = offset_topic;
    this.group = group;
    this.cluster_id = cluster_id;
    this.connectors = connectors;
  }

  public String statusTopicString() {
    return status_topic.orElse(DEFAULT_CONNECT_STATUS_TOPIC);
  }

  public String offsetTopicString() {
    return offset_topic.orElse(DEFAULT_CONNECT_OFFSET_TOPIC);
  }

  public String configsTopicString() {
    return configs_topic.orElse(DEFAULT_CONNECT_CONFIGS_TOPIC);
  }

  public String groupString() {
    return group.orElse(DEFAULT_CONNECT_GROUP);
  }

  public void setStatus_topic(Optional<String> status_topic) {
    this.status_topic = status_topic;
  }

  public void setOffset_topic(Optional<String> offset_topic) {
    this.offset_topic = offset_topic;
  }

  public void setConfigs_topic(Optional<String> configs_topic) {
    this.configs_topic = configs_topic;
  }

  public void setGroup(Optional<String> group) {
    this.group = group;
  }

  public void setCluster_id(Optional<String> cluster_id) {
    this.cluster_id = cluster_id;
  }

  public Optional<String> getCluster_id() {
    return cluster_id;
  }

  public Optional<String> getStatus_topic() {
    return status_topic;
  }

  public Optional<String> getOffset_topic() {
    return offset_topic;
  }

  public Optional<String> getConfigs_topic() {
    return configs_topic;
  }

  public Optional<String> getGroup() {
    return group;
  }

  public Optional<List<String>> getConnectors() {
    return connectors;
  }

  public void setConnectors(Optional<List<String>> connectors) {
    this.connectors = connectors;
  }
}
