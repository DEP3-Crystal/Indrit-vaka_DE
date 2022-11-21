package com.proxy;

import com.crystal.anotation.GetRole;
import com.crystal.model.User;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class ServicesInvocationHandler implements InvocationHandler {
    private Services services;
    private User user;
    private String userRole;

    public ServicesInvocationHandler(Services services, User user) {
        this.services = services;
        this.user = user;
        this.userRole = user.getRole();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String role = "";
        if (method.getAnnotation(GetRole.class) != null) {
            role = method.getAnnotation(GetRole.class).role();
        }

        if (role.equalsIgnoreCase("") || userRole.equalsIgnoreCase("admin") || role.equalsIgnoreCase(userRole)) {
            Object result = method.invoke(services, args);
            return result;
        } else {
            throw new RuntimeException("You don't have access to access this method: " + method.getName());
        }


    }
}
