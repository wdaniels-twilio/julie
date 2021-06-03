package com.purbon.kafka.topology.validation;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;

public interface SchemaValidation extends Validation {
    void valid(final SchemaRegistryManager schemaRegistryManager, Topic topic, TopicSchemas schema) throws ValidationException;
}
