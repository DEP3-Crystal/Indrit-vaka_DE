package com.crystal.proxy;

import com.crystal.anotation.GetRole;

public interface IServices {
    @GetRole(role = "user")
    public void addOneUser(int n);

    @GetRole(role = "admin")
    public void sqrtAdmin(int n);

    public void multiply(int n);

}
