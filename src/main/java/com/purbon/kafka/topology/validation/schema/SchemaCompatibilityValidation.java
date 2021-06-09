package com.purbon.kafka.topology.validation.schema;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.schema.Subject;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import com.purbon.kafka.topology.validation.SchemaValidation;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaCompatibilityValidation implements SchemaValidation {
  private static final Logger LOGGER = LogManager.getLogger(SchemaCompatibilityValidation.class);

  private SchemaRegistryManager schemaRegistryManager;

  @Override
  public void valid(
      final SchemaRegistryManager schemaRegistryManager, Topic topic, TopicSchemas schema)
      throws ValidationException {

    if (null == topic) {
      throw new ValidationException("Schema validation failed due to null topic.");
    }
    if (null == schema) {
      throw new ValidationException("Schema validation failed due to null schema.");
    }
    this.schemaRegistryManager = schemaRegistryManager;
    try {
      var validationResult = false;
      validationResult = validateSchemaIfExists(schema.getKeySubject(), topic);
      if (!validationResult) {
        throw new ValidationException(
            String.format(
                "Schema with keySubject %s and topic: %s failed " + "compatibility check",
                schema.getKeySubject() == null
                    ? "null"
                    : schema.getKeySubject().buildSubjectName(topic),
                topic.getName()));
      }
      validationResult = validateSchemaIfExists(schema.getValueSubject(), topic);
      if (!validationResult) {
        throw new ValidationException(
            String.format(
                "Schema with valueSubject %s and topic: %s failed " + "compatibility check",
                schema.getValueSubject() == null
                    ? "null"
                    : schema.getValueSubject().buildSubjectName(topic),
                topic.getName()));
      }
    } catch (IOException ioex) {
      LOGGER.debug("IOException while trying to check schema compatibility.", ioex);
      throw new ValidationException(ioex.getMessage());
    }
  }

  private boolean validateSchemaIfExists(Subject subject, Topic topic) throws IOException {
    if (subject.hasSchemaFile()) {
      String keySchemaFile = subject.getSchemaFile();
      String subjectName = subject.buildSubjectName(topic);
      return schemaRegistryManager.validate(subjectName, keySchemaFile, subject.getFormat());
    }
    return false;
  }
}
