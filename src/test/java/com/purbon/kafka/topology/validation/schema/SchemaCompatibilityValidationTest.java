package com.purbon.kafka.topology.validation.schema;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Impl.TopicImpl;
import com.purbon.kafka.topology.model.schema.TopicSchemas;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SchemaCompatibilityValidationTest {

    @Mock
    private SchemaRegistryManager mockedSchemaRegistry;

    @Test(expected = ValidationException.class)
    public void testSchemaValidationFailIfBothNullTopicAndSchema()
        throws ValidationException {
        //Given
        var instance = new SchemaCompatibilityValidation();

        //When
        instance.valid(mockedSchemaRegistry, null, null);

        //Then Should throw validation exception
        fail();
    }

    @Test(expected = ValidationException.class)
    public void testSchemaValidationFailIfNullTopic()
        throws ValidationException {
        //Given
        var instance = new SchemaCompatibilityValidation();

        //When
        instance.valid(mockedSchemaRegistry, null, new TopicSchemas("", ""));

        //Then Should throw validation exception
        fail();
    }

    @Test(expected = ValidationException.class)
    public void testSchemaValidationFailIfNullSchema()
        throws ValidationException {
        //Given
        var instance = new SchemaCompatibilityValidation();

        //When
        instance.valid(mockedSchemaRegistry, new TopicImpl(), null);

        //Then Should throw validation exception
        fail();
    }

    @Test
    public void validationFailureMessageForKeySubjectIsCorrect(){
        //Given
        var instance = new SchemaCompatibilityValidation();
        var expectedMessage = "Schema with keySubject null.default-key and topic: default failed compatibility "
                              + "check";
        var schema = new TopicSchemas("testKey", "testValue");
        when(mockedSchemaRegistry.validate("null.default-key", "testKey", "AVRO")).thenReturn(false);
        //When
        try {
            instance.valid(mockedSchemaRegistry, new TopicImpl(), new TopicSchemas("testKey", "testValue"));
            fail(); // exception should have been thrown.
        }catch(ValidationException ve){
            //Then
            Assert.assertEquals(expectedMessage, ve.getMessage());
        }
    }

    @Test
    public void validationFailureMessageForValueSubjectIsCorrect(){
        //Given
        var instance = new SchemaCompatibilityValidation();
        var expectedMessage = "Schema with valueSubject null.default-value and topic: default failed compatibility "
                              + "check";
        var schema = new TopicSchemas("testKey", "testValue");
        when(mockedSchemaRegistry.validate("null.default-value", "testValue", "AVRO")).thenReturn(false);
        when(mockedSchemaRegistry.validate("null.default-key", "testKey", "AVRO")).thenReturn(true);
        //When
        try {
            instance.valid(mockedSchemaRegistry, new TopicImpl(), new TopicSchemas("testKey", "testValue"));
            fail(); // exception should have been thrown.
        }catch(ValidationException ve){
            //Then
            Assert.assertEquals(expectedMessage, ve.getMessage());
        }
    }

}
