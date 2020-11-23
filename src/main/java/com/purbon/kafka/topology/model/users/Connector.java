package com.purbon.kafka.topology.model.users;

import com.purbon.kafka.topology.model.users.connector.ConnectorAccount;
import com.purbon.kafka.topology.model.users.connector.ConnectorEntity;
import java.util.Collections;
import java.util.List;

public class Connector {

  private List<ConnectorAccount> accounts;
  private List<ConnectorEntity> entities;

  public Connector() {
    this(Collections.emptyList(), Collections.emptyList());
  }

  public Connector(List<ConnectorAccount> accounts) {
    this(accounts, Collections.emptyList());
  }

  public Connector(List<ConnectorAccount> accounts, List<ConnectorEntity> entities) {
    this.accounts = accounts;
    this.entities = entities;
  }

  public List<ConnectorAccount> getAccounts() {
    return accounts;
  }

  public List<ConnectorEntity> getEntities() {
    return entities;
  }

  public boolean isEmpty() {
    return accounts.isEmpty() && entities.isEmpty();
  }
}
