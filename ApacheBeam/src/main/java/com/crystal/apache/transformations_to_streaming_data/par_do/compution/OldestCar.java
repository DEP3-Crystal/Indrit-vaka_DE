package com.crystal.apache.transformations_to_streaming_data.par_do.compution;

import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.joda.time.LocalDateTime;

public class OldestCar {
    private static final String HEADER = "car,price,body,mileage,engV,engType,registration,year,model,drive";


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        pipeline.apply("Reading data", TextIO.read().from(root + "source/car_ads/car_ads_*.csv"))
                .apply("Filtering header", ParDo.of(new RemoveHeadersFn()))
                .apply("Deserializing", ParDo.of(new DeserializeCarFn()))
                .apply("Oldest car", Max.globally((o1, o2) -> getCarAge(o1) - getCarAge(o2)
                ))
                .apply("Printing oldest car", MapElements
                        .via(new SimpleFunction<Car, Void>() {
                            @Override
                            public Void apply(Car car) {
                                System.out.println(HEADER);
                                System.out.println(car);
                                System.out.println(getCarAge(car)+ "years old");
                                return null;
                            }
                        }))
        ;
        pipeline.run().waitUntilFinish();

    }

    public static int getCarAge(Car car) {
        return LocalDateTime.now().getYear() - car.getYear();
    }

    private static class RemoveHeadersFn extends DoFn<String, String> {
        @ProcessElement
        public void filterHeader(@Element String el, OutputReceiver<String> o) {
            if (el.contains(HEADER) || el.isEmpty()) return;
            o.output(el);
        }
    }


    private static class FilterByMark extends DoFn<Car, Car> {
        private final String make;

        public FilterByMark(String make) {
            this.make = make;
        }

        @ProcessElement
        public void filter(ProcessContext c) {
            var car = c.element();
            if (car.getMake().equals(make)) {
                c.output(car);
            }
        }
    }

    private static class PrintCarFn extends DoFn<Car, Car> {
        @ProcessElement
        public void printCar(ProcessContext c) {
            System.out.println(c.element());
            c.output(c.element());
        }
    }
}
