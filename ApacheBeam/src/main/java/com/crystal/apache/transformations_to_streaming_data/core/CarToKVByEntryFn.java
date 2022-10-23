package com.crystal.apache.transformations_to_streaming_data.core;

import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class CarToKVByEntryFn extends DoFn<Car, KV<String, Car>> {
    private String entries;
    private String carEntry;

    public CarToKVByEntryFn(String entries, String carEntry) {
        this.entries = entries;
        this.carEntry = carEntry;
    }

    @ProcessElement
    public void transformToKV(ProcessContext c) {
        Car car = c.element();
        //entries example
        //car,price,body,mileage,engV,engType,registration,year,model,drive

        //extracting entries
        List<String> extractedEntries = List.of(this.entries.split(","));
        //getting index of required entry
        int carEntryIndex = extractedEntries.indexOf(carEntry);
        StringWriter stringWriter = new StringWriter();
        try {
            CSVPrinter parser = new CSVPrinter(stringWriter, CSVFormat.DEFAULT);
            parser.printRecord(car);
            System.out.println(stringWriter);
            String key = stringWriter.toString().split(",")[carEntryIndex];
            c.output(KV.of(key, car));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
