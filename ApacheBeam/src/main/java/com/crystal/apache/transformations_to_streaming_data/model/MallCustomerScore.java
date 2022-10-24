package com.crystal.apache.transformations_to_streaming_data.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
@Data
@Builder
public class MallCustomerScore implements Serializable {
    //CustomerID,Spending Score
    private int customerID;
    private double spendingScore;
}
