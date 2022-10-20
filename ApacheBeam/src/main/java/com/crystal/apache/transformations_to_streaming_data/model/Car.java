package com.crystal.apache.transformations_to_streaming_data.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class Car implements Serializable {
    //car,price,body,mileage,engV,engType,registration,year,model,drive
    private String mark;
    private float price;
    private String body;
    private String mileage;
    private String engV;
    private String engType;
    private String registration;
    private int year;
    private String model;
    private String drive;

    @Override
    public String toString() {
        return
                mark + ',' +
                        price +
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
