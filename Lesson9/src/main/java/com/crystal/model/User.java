package com.crystal.model;


public class User implements IUser{
    private String name;
    private int age;
    private ThreadLocal<String> role = new ThreadLocal<>();

    public User(String name, int age, String role) {
        this.name = name;
        this.age = age;
        this.role.set(role);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getRole() {
        return role.get();
    }

    public void setRole(String role) {
        this.role.set(role);
    }
}
