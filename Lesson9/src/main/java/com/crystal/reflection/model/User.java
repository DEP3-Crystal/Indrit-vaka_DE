package com.crystal.reflection.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {
    private String name;
    private int age;
    private String role;

    public User(String name, int age, String role) {
        this.name = name;
        this.age = age;
        this.role = role;
    }
}
