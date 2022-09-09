package com.crystal.proxy;

import com.crystal.anotation.GetRole;

public class Services implements IServices {
    @Override
    public void addOneUser(int n) {
        System.out.println("I can be executed by User => " + (n + 1));
    }
    @Override
    public void sqrtAdmin(int n) {
        System.out.println("I can be executed only by Admin => " + n * n);
    }
    @Override
    public void multiply(int n) {
        System.out.println("i have no annotation => " + n * 5);
    }

}
