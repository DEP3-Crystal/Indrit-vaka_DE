package com.crystal.apache.transformations_to_streaming_data.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
@Data
@Builder
public class Customer implements Serializable {
    private int customerID;
    private String gender;
    private int age;
    private double annualIncome;
    private double spendingScore;
}
