package com.crystal.apache.transformations_to_streaming_data.par_do;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Filtering {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from("ApacheBeam/src/main/resources/source/car_ads/car_ads_*.csv"))
                .apply(ParDo.of(new RemoveHeader()))
                .apply(ParDo.of(new FilterDieselAndSedanFn()))
                .apply(ParDo.of(new FilterPriceFn(2000)))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void printElements(@Element String el,OutputReceiver<Void> outputReceiver){
                        System.out.println(el);
                    }
                }));

        pipeline.run().waitUntilFinish();

    }

    private static class RemoveHeader extends DoFn<String, String> {
        private CharSequence header = "car,price,body,mileage,engV,engType,registration,year,model,drive";

        @ProcessElement
        public void filterHeader(ProcessContext c) {
            if (c.element().contains(header)) return;
            c.output(c.element());
        }
    }

    private static class FilterDieselAndSedanFn extends DoFn<String, String> {
        @ProcessElement
        public void filter(ProcessContext c) {
            if (!c.element().contains("Diesel") && !c.element().contains("Sedan")) return;
            c.output(c.element());
        }
    }

    private static class FilterPriceFn extends DoFn<String, String> {
        private final int priceThreshold;

        public FilterPriceFn(int priceThreshold) {
            this.priceThreshold = priceThreshold;
        }

        @ProcessElement
        public void filterByPrice(ProcessContext c) {
            float price = Float.parseFloat(c.element().split(",")[1]);
            if (price < priceThreshold) return;
            c.output(c.element());
        }
    }
}
