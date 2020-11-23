package com.purbon.kafka.topology.serdes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.purbon.kafka.topology.TopologyBuilderConfig;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.connector.ConnectorAccount;
import com.purbon.kafka.topology.model.users.connector.ConnectorEntity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConnectorCustomDeserializer extends StdDeserializer<Connector> {

  private TopologyBuilderConfig config;

  public ConnectorCustomDeserializer(TopologyBuilderConfig config) {
    this(null, config);
  }

  private ConnectorCustomDeserializer(Class<?> clazz, TopologyBuilderConfig config) {
    super(clazz);
    this.config = config;
  }

  @Override
  public Connector deserialize(JsonParser parser, DeserializationContext context)
      throws IOException, JsonProcessingException {
    JsonNode rootNode = parser.getCodec().readTree(parser);
    List<ConnectorAccount> accounts = new ArrayList<>();
    List<ConnectorEntity> entities = new ArrayList<>();
    if (containsEntities(rootNode)) {
      // do new version parsing
      JsonNode accountsNode = rootNode.get("accounts");
      accountsNode.elements().forEachRemaining(accountsParser(accounts));
      JsonNode instancesNode = rootNode.get("entities");
      instancesNode.elements().forEachRemaining(entitiesParser(entities));

    } else {
      // do old version parsing
      ConnectorAccount account = new ConnectorAccount(rootNode);
      accounts.add(account);
    }
    return new Connector(accounts, entities);
  }

  private java.util.function.Consumer<JsonNode> accountsParser(List<ConnectorAccount> accounts) {
    return node -> {
      ConnectorAccount account = new ConnectorAccount(node);
      accounts.add(account);
    };
  }

  private java.util.function.Consumer<JsonNode> entitiesParser(List<ConnectorEntity> entities) {
    return node -> {
      ConnectorEntity entity = new ConnectorEntity(node);
      entities.add(entity);
    };
  }

  private boolean containsEntities(JsonNode node) {
    return node.has("accounts") || node.has("entities");
  }
}
