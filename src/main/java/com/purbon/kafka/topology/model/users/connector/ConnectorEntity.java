package com.purbon.kafka.topology.model.users.connector;

import com.fasterxml.jackson.databind.JsonNode;

public class ConnectorEntity {

  private String path;

  public ConnectorEntity(JsonNode node) {
    this(node.asText());
  }

  public ConnectorEntity(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }
}
