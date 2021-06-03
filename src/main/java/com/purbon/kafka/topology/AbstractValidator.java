package com.purbon.kafka.topology;

import com.purbon.kafka.topology.validation.TopicValidation;
import com.purbon.kafka.topology.validation.TopologyValidation;
import com.purbon.kafka.topology.validation.Validation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractValidator implements ValidatableTopology {

    private final Configuration configuration;

    protected AbstractValidator(Configuration configuration) {
        this.configuration = configuration;
    }

    protected List<Validation> validations() {
        return configuration.getTopologyValidations().stream()
                            .map(
                                validationClass -> {
                                    try {
                                        Class<?> clazz = getValidationClazz(validationClass);
                                        if (null == clazz) {
                                            throw new IOException(
                                                String.format(
                                                    "Could not find validation class '%s' in class path. "
                                                    + "Please use the fully qualified class name and check your "
                                                    + "config.",
                                                    validationClass));
                                        }

                                        Constructor<?> constructor = clazz.getConstructor();
                                        Object instance = constructor.newInstance();
                                        if (instance instanceof TopologyValidation) {
                                            return (TopologyValidation) instance;
                                        } else if (instance instanceof TopicValidation) {
                                            return (TopicValidation) instance;
                                        } else {
                                            throw new IOException(
                                                "invalid validation type specified " + validationClass);
                                        }
                                    } catch (Exception ex) {
                                        throw new IllegalStateException(
                                            "Failed to load topology validations from class path", ex);
                                    }
                                })
                            .collect(Collectors.toList());
    }

    private Class<?> getValidationClazz(String validationClass) {
        try {
            return Class.forName(validationClass);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
