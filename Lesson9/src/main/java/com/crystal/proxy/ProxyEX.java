package com.crystal.proxy;

import com.crystal.model.User;

import java.lang.reflect.Proxy;

public class ProxyEX {
    public static void main(String[] args) {
        User user = new User("Indrit", 21, "guest");
        Services services = new Services();
        ServicesInvocationHandler invocationHandler = new ServicesInvocationHandler(services, user);

        IServices proxyInstance = (IServices)Proxy.newProxyInstance(Services.class.getClassLoader(), new Class[]{IServices.class}, invocationHandler);

        //No annotations
        proxyInstance.multiply(5);
        //user privileges
        proxyInstance.addOneUser(5);
        //Admin privileges
        proxyInstance.sqrtAdmin(5);

    }
}
