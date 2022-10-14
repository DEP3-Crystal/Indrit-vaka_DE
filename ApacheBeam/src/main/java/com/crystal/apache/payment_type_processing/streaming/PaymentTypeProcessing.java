package com.crystal.apache.payment_type_processing.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Collections;

public class PaymentTypeProcessing {
    private static String CSV_HEADER = "Date,Product,Card,Country";

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read()
                        .from("ApacheBeam/src/main/resources/source/SalesJan2009.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("Extract payment Method", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via(line -> Collections.singleton(line.split(",")[3])))
                .apply("Counting per element", Count.perElement())
                .apply("Format result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(el -> el.getKey() + ":" + el.getValue()))
                .apply("Writing results", TextIO.write()
                        .to("ApacheBeam/src/main/resources/sink/payment_processing/payment_type_count")
                        //.withNumShards(2)
                        .withoutSharding()
                        .withShardNameTemplate("-SSS")
                        .withHeader("Card,Count")
                )

        ;
        pipeline.run();
        System.out.println("Processing finished");
    }

    private static class FilterHeaderFn extends DoFn<String, String> {
        private String header;

        public FilterHeaderFn(String csvHeader) {

            header = csvHeader;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            if (!row.isEmpty() && !row.contains(header)) {
                c.output(row);
            }
        }
    }

}
