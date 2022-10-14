package com.crystal.apache.fixed_window;


import com.crystal.apache.fixed_window.model.EnergyConsumption;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;

public class FixedWindowEnergyConsumption {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadEnergyConsumption",
                        TextIO.read().from("ApacheBeam/src/main/resources/source/AEP_hourly.csv"))

                .apply("ParseEnergyData",
                        parseEnergyData())
                .apply("toString", MapElements
                        .into(TypeDescriptors.strings())
                        .via(us -> us.asCSVRow(",")))
                .apply("WriteToFile", TextIO
                        .write()
                        .to("ApacheBeam/src/main/resources/sink/csv_output").withSuffix(".csv")
                        .withNumShards(1)
                        .withWindowedWrites()
                );

        pipeline.run();
    }

    private static ParDo.SingleOutput<String, EnergyConsumption> parseEnergyData() {
        return ParDo.of(new DoFn<String, EnergyConsumption>() {
            private final String[] FILE_HEADER_MAPPING = {"Datetime", "AEP_MW"};

            @ProcessElement
            public void parseEnergyData(ProcessContext c) throws IOException {
                final CSVParser parser = new CSVParser(new StringReader(
                        c.element()),
                        CSVFormat.Builder.create()
                                .setDelimiter(',')
                                .setHeader(FILE_HEADER_MAPPING)
                                //.setSkipHeaderRecord(true)
                                .build());
                CSVRecord record = parser.getRecords().get(0);
                if (record.get("Datetime").contains("Datetime")) {
                    return;
                }
                DateTimeZone timeZone = DateTimeZone.forID("Asia/Kolkata");
                DateTime date = LocalDateTime.parse(record.get("Datetime").trim(),
                        DateTimeFormat.forPattern("yyy-MM-dd HH:mm:ss")).toDateTime(timeZone);
                EnergyConsumption energyConsumption = new EnergyConsumption();
                energyConsumption.setDateTime(date.toInstant());
                energyConsumption.setEnergyConsumption(Double.valueOf(record.get("AEP_MW")));
                c.output(energyConsumption);
            }
        });
    }


}
