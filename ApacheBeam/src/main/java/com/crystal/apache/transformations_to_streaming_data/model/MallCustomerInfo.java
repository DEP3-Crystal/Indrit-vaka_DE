package com.crystal.apache.transformations_to_streaming_data.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Builder
@Data
public class MallCustomerInfo implements Serializable {
    //CustomerID,Gender,Age,Annual_Income
    private int customerID;
    private String gender;
    private int age;
    private double annualIncome;

    @Override
    public String toString() {
        return customerID + ","
                + gender + ','
                + age + ','
                + annualIncome;
    }
}
