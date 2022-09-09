package com.crystal.reflection;

import com.crystal.anotation.GetRole;
import com.crystal.model.User;
import com.crystal.proxy.Services;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CallingMethodsBasedOnAnnotation {
    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException {
        User userADM = new User("Indrit", 21, "admin");
        User user = new User("add", 21, "user");
        Services services = new Services();

        Method[] methods = Services.class.getDeclaredMethods();

        executeMethods(userADM, services, methods);
        executeMethods(user, services, methods);
    }

    private static void executeMethods(User user, Services services, Method[] methods) throws IllegalAccessException, InvocationTargetException {
        for (Method method : methods) {
            if (method.getAnnotation(GetRole.class) != null) {
                String role = method.getAnnotation(GetRole.class).role();
                if (user.getRole().equalsIgnoreCase("admin") || role.equalsIgnoreCase(user.getRole()) || role.equalsIgnoreCase("")) {
                    method.setAccessible(true);
                    method.invoke(services, 2);
                } else {
                    throw new RuntimeException("you don't have access");
                }
            } else {

                method.setAccessible(true);
                method.invoke(services, 2);
            }
        }
    }


}

