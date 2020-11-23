package com.purbon.kafka.topology.model.users.connector;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

public class ConnectorEntity {

  private String path;
  private String name;

  public ConnectorEntity() {
    this("", "");
  }

  public ConnectorEntity(JsonNode node) {
    this(node.get("name").asText(), node.get("path").asText());
  }

  public ConnectorEntity(String name, String path) {
    this.name = name;
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConnectorEntity)) {
      return false;
    }
    ConnectorEntity entity = (ConnectorEntity) o;
    return Objects.equals(getPath(), entity.getPath()) &&
        Objects.equals(getName(), entity.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPath(), getName());
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ConnectorEntity{");
    sb.append("path='").append(path).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
