package com.crystal.apache.transformations_to_streaming_data.par_do;

import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FilteringUsingModel {
    private static final String HEADER = "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        pipeline.apply("Reading data", TextIO.read().from(root + "source/car_ads/car_ads_*.csv"))
                .apply("Filtering header", ParDo.of(new RemoveHeadersFn()))
                .apply("Deserializing", ParDo.of(new DeserializeCarFn()))
                .apply("Filtering by mark", ParDo.of(new FilterByMark("Land Rover")))
                .apply("Showing car", ParDo.of(new PrintCarFn()))
                .apply("Serializing", MapElements
                        .into(TypeDescriptors.strings())
                        .via(Car::toString))
                .apply("saving to file", TextIO.write()
                        .to(root + "sink/car_ads/Land Rover")
                        .withSuffix(".csv")
                        .withShardNameTemplate("-SSS")
                )
        ;
        pipeline.run().waitUntilFinish();

    }

    private static class RemoveHeadersFn extends DoFn<String, String> {
        @ProcessElement
        public void filterHeader(@Element String el, OutputReceiver<String> o) {
            if (el.contains(HEADER) || el.isEmpty()) return;
            o.output(el);
        }
    }

    private static class DeserializeCarFn extends DoFn<String, Car> {
        @ProcessElement
        public void deserialize(ProcessContext c) {
            String[] entries = c.element().split(",");
            if (entries.length < 10) {
                //System.out.println("Missing entries at line: " +c.element() );
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

    private static class FilterByMark extends DoFn<Car, Car> {
        private final String mark;

        public FilterByMark(String mark) {
            this.mark = mark;
        }

        @ProcessElement
        public void filter(ProcessContext c) {
            var car = c.element();
            if (car.getMark().equals(mark)) {
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
