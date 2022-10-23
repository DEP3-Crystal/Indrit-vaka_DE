package com.crystal.apache.transformations_to_streaming_data.core;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class ToKVByEntryFnOld<T> extends DoFn<T, KV<String, T>> {
    private String csvEntries;
    private String entry;

    public ToKVByEntryFnOld(String csvEntries, String entry) {
        this.csvEntries = csvEntries;
        this.entry = entry;
    }

    @ProcessElement
    public void transformToKV(ProcessContext c) {
        T t = c.element();
        //entries example
        //t,price,body,mileage,engV,engType,registration,year,model,drive

        //extracting entries
        List<String> entries = List.of(csvEntries.split(","));
        //getting index of required entry
        int carEntryIndex = entries.indexOf(entry);
        StringWriter stringWriter = new StringWriter();
        try {
            CSVPrinter parser = new CSVPrinter(stringWriter, CSVFormat.DEFAULT);
            parser.printRecord(t);
            System.out.println(stringWriter);
            String key = stringWriter.toString().split(",")[carEntryIndex];
            c.output(KV.of(key, t));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
