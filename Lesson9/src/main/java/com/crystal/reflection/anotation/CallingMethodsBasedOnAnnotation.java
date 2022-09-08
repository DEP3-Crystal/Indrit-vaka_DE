package com.crystal.reflection.anotation;

import com.crystal.reflection.model.User;

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

class Services {
    @GetRole(role = "user")
    public void addOne(int n) {
        System.out.println("Im user => " + (n + 1));
    }

    @GetRole(role = "admin")
    public void sqrt(int n) {
        System.out.println("i am admin => " + n * n);
    }

    public void multiply(int n) {
        System.out.println("i have no annotation => " + n * 5);
    }

}
