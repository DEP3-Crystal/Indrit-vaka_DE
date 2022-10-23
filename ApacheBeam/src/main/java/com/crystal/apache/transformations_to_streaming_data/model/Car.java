package com.crystal.apache.transformations_to_streaming_data.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class Car implements Serializable {
    private String mark;
    private double price;
    private String body;
    private String mileage;
    private String engV;
    private String engType;
    private String registration;
    private int year;
    private String model;
    private String drive;
    //car,price,body,mileage,engV,engType,registration,year,model,drive

    @Override
    public String toString() {
        return mark + ',' +
                price + "," +
                body + ',' +
                mileage + ',' +
                engV + ',' +
                engType + ',' +
                registration + ',' +
                year + ',' +
                model + ',' +
                drive + ',' + '\n';
    }
}
