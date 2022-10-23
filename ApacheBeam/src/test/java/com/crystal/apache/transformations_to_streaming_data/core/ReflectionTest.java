package com.crystal.apache.transformations_to_streaming_data.core;


import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReflectionTest {

    @Test
    public void givenObject_whenGetsFieldNamesAtRuntime_thenCorrect() throws NoSuchFieldException, IllegalAccessException {
        Person person = Person.builder()
                .name("Indrit")
                .age(21)
                .build();
        Field[] fields = person.getClass().getDeclaredFields();
        Field name = person.getClass().getDeclaredField("name");
        name.setAccessible(true);
        Assert.assertEquals("Indrit",name.get(person));
    }



    private static List<String> getFieldNames(Field[] fields) {
        List<String> fieldNames = new ArrayList<>();
        for (Field field : fields)
            fieldNames.add(field.getName());
        return fieldNames;
    }

}