package com.crystal.apache.transformations_to_streaming_data.par_do.compution;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.time.LocalDate;

public class Computations {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        final String header = "car,price,body,mileage,engV,engType,registration,year,model,drive";

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from("ApacheBeam/src/main/resources/source/car_ads/car_ads_*.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeader(header)))
                .apply("Make and the year", ParDo.of(new MakeAndYearKVFn()))
                .apply("Oldest car per make", Max.perKey())
                .apply("Printing result", ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                    @ProcessElement
                    public void printCar(ProcessContext c){
                        KV<String, Integer> makeYear = c.element();
                        System.out.println("The year of oldest car : " +makeYear.getValue() + " per " + makeYear.getKey() );
                        c.output(c.element());
                    }
                }))
                .apply("Globally min", Max.globally((o1, o2) -> o1.getValue() - o2.getValue()))
                .apply("Printing oldest car", MapElements
                        .via(new SimpleFunction<KV<String, Integer>, Void>() {
                            @Override
                            public Void apply(KV<String, Integer> input) {
                                System.out.println(input);
                                return  null;
                            }
                        }))
        ;
        pipeline.run().waitUntilFinish();

    }

    private static class RemoveHeader extends DoFn<String, String> {

        private final String header;

        public RemoveHeader(String header) {

            this.header = header;
        }

        @ProcessElement
        public void filterHeader(ProcessContext c) {
            if (c.element().contains(header)) return;
            c.output(c.element());
        }
    }

    private static class MakeAndPriceKVFn extends DoFn<String, KV<String, Double>> {
        //"car,price,body,mileage,engV,engType,registration,year,model,drive"
        @ProcessElement
        public void getMakeAndPrice(ProcessContext c) {
            String[] entries = c.element().split(",");
            String make = entries[0];
            Double price = Double.parseDouble(entries[1]);
            c.output(KV.of(make, price));
        }
    }

    private static class MakeAndYearKVFn extends DoFn<String, KV<String, Integer>> {
        //"car,price,body,mileage,engV,engType,registration,year,model,drive"
        @ProcessElement
        public void getYearPerCar(ProcessContext c) {
            String[] entries = c.element().split(",");
            String make = entries[0];
            int year = Integer.parseInt(entries[7]);
            int currentYear = LocalDate.now().getYear();
            int age = currentYear - year;
            c.output(KV.of(make, age));
        }
    }
}
