package com.crystal.apache.transformations_to_streaming_data.core;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Person {
    private String name;
    private int age;
}
