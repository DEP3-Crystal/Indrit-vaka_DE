package com.crystal.apache.transformations_to_streaming_data.core;

import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.transforms.DoFn;

public class DeserializeCarFn extends DoFn<String, Car> {
    @ProcessElement
    public void deserialize(ProcessContext c) {
        String[] entries = c.element().split(",");
        if (entries.length < 10) {
            return;
        }
        String mark = entries[0];
        float price = Float.parseFloat(entries[1]);
        String body = entries[2];
        String mileage = entries[3];
        String engV = entries[4];
        String engType = entries[5];
        String registration = entries[6];
        int year = Integer.parseInt(entries[7]);
        String model = entries[8];
        String drive = entries[9];
        Car car = Car.builder()
                .mark(mark)
                .price(price)
                .body(body)
                .mileage(mileage)
                .engV(engV)
                .engType(engType)
                .registration(registration)
                .year(year)
                .model(model)
                .drive(drive).build();
        c.output(car);

    }
}
