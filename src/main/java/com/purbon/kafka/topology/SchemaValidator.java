package com.purbon.kafka.topology;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import com.purbon.kafka.topology.utils.Either;
import com.purbon.kafka.topology.validation.SchemaValidation;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaValidator extends AbstractValidator {
  private static final Logger LOGGER = LogManager.getLogger(SchemaValidator.class);

  private final SchemaRegistryManager schemaRegistryManager;
  private final List<String> managedPrefixes;

  public SchemaValidator(
      final SchemaRegistryManager schemaRegistryManager, final Configuration configuration) {
    super(configuration);
    this.schemaRegistryManager = schemaRegistryManager;
    this.managedPrefixes = configuration.getTopicManagedPrefixes();
  }

  public List<String> validate(Topology topology) {
    Map<String, Topic> topics = parseMapOfTopics(topology);
    Stream<Either<Boolean, ValidationException>> streamOfSchemaResults =
        validations().stream()
            .filter(SchemaValidation.class::isInstance)
            .map(SchemaValidation.class::cast)
            .map(
                validation -> {
                  try {
                    for (var topic : topics.values()) {
                      for (var schema : topic.getSchemas()) {
                        validation.valid(schemaRegistryManager, topic, schema);
                      }
                    }
                    return Either.Left(true);
                  } catch (ValidationException validationError) {
                    return Either.Right(validationError);
                  }
                });

    return streamOfSchemaResults
        .filter(Either::isRight)
        .map(either -> either.getRight().get().getMessage())
        .collect(Collectors.toList());
  }

  private Map<String, Topic> parseMapOfTopics(Topology topology) {
    return topology.getProjects().stream()
        .flatMap(project -> project.getTopics().stream())
        .filter(this::matchesPrefixList)
        .collect(Collectors.toMap(Topic::toString, topic -> topic));
  }

  private boolean matchesPrefixList(Topic topic) {
    return matchesPrefixList(topic.toString());
  }

  private boolean matchesPrefixList(String topic) {
    boolean matches =
        managedPrefixes.isEmpty() || managedPrefixes.stream().anyMatch(topic::startsWith);
    LOGGER.debug("Topic {} matches {} with {}", topic, matches, managedPrefixes);
    return matches;
  }
}
