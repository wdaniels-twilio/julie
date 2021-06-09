package com.purbon.kafka.topology;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.utils.Either;
import com.purbon.kafka.topology.validation.TopicValidation;
import com.purbon.kafka.topology.validation.TopologyValidation;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TopologyValidator extends AbstractValidator {

  private static final Logger LOGGER = LogManager.getLogger(TopologyValidator.class);

  public TopologyValidator(final Configuration config) {
    super(config);
  }

  public List<String> validate(Topology topology) {
    Stream<Either<Boolean, ValidationException>> streamOfTopologyResults =
        validations().stream()
            .filter(TopologyValidation.class::isInstance)
            .map(TopologyValidation.class::cast)
            .map(
                validation -> {
                  try {
                    validation.valid(topology);
                    return Either.Left(true);
                  } catch (ValidationException validationError) {
                    return Either.Right(validationError);
                  }
                });

    List<TopicValidation> listOfTopicValidations =
        validations().stream()
            .filter(TopicValidation.class::isInstance)
            .map(TopicValidation.class::cast)
            .collect(Collectors.toList());

    Stream<Topic> streamOfTopics =
        topology.getProjects().stream()
            .flatMap((Function<Project, Stream<Topic>>) project -> project.getTopics().stream());

    Stream<Either<Boolean, ValidationException>> streamOfTopicResults =
        streamOfTopics.flatMap(
            (Function<Topic, Stream<Either<Boolean, ValidationException>>>)
                topic ->
                    listOfTopicValidations.stream()
                        .map(
                            validation -> {
                              try {
                                validation.valid(topic);
                                return Either.Left(true);
                              } catch (ValidationException ex) {
                                return Either.Right(ex);
                              }
                            }));

    return Stream.concat(streamOfTopologyResults, streamOfTopicResults)
        .filter(Either::isRight)
        .map(either -> either.getRight().isPresent() ? either.getRight().get().getMessage() : null)
        .collect(Collectors.toList());
  }
}
