package com.purbon.kafka.topology;

import com.purbon.kafka.topology.model.Topology;
import java.util.List;

public interface ValidatableTopology {
  List<String> validate(Topology topology);
}
