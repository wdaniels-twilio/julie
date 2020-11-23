package com.purbon.kafka.topology;

import com.purbon.kafka.topology.api.connect.KafkaConnectClient;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.connector.ConnectorEntity;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectorManager {

  private final KafkaConnectClient client;

  public ConnectorManager(KafkaConnectClient client) {
    this.client = client;
  }

  public void apply(Topology topology, ExecutionPlan plan) {

    List<ConnectorEntity> connectors =
        topology.getProjects().stream()
            .flatMap(p -> p.getConnectors().getEntities().stream())
            .collect(Collectors.toList());
  }
}
