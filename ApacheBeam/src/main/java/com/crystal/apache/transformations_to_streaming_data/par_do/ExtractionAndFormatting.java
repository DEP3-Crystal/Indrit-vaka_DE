package com.crystal.apache.transformations_to_streaming_data.par_do;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

public class ExtractionAndFormatting {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        final String header = "car,price,body,mileage,engV,engType,registration,year,model,drive";

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from("ApacheBeam/src/main/resources/source/car_ads/car_ads_*.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeader(header)))
                .apply("Getting Mark and price into KV", ParDo.of(new MakeAndPriceKVFn()))
                .apply("Average", Mean.perKey())
                        .apply("printing results",ParDo.of(new DoFn<KV<String, Double>, Void>() {
                           @ProcessElement
                            public void printResult(ProcessContext c){
                               System.out.println(c.element());
                           }
                        }));
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
}
