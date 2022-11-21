package com.crystal.apache.transformations_to_streaming_data.composit;

import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.group_by.PrintResultFn;
import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CompositeV2 {
    static String header = "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        String path = "ApacheBeam/src/main/resources/source/car_ads/car_ads_*.csv";
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from(path))
                .apply("KV", new MakePriceKVTransform())

                .apply("average of price per make", Mean.perKey())
                .apply("Printing result", ParDo.of(new PrintResultFn<>()));

        pipeline.run().waitUntilFinish();
    }

    private static class MakePriceKVTransform extends PTransform<PCollection<String>, PCollection<KV<String, Double>>> {

        @Override
        public PCollection<KV<String, Double>> expand(PCollection<String> input) {
            return input.apply("", ParDo.of(new RemoveHeadersFn(header)))
                    .apply("deserialization", ParDo.of(new DeserializeCarFn()))
                    .apply("KV", ParDo.of(new ToKVByEntryFn<>("make")))
                    .apply("getting price Only", MapElements.via(new SimpleFunction<KV<String, Car>, KV<String, Double>>() {
                        @Override
                        public KV<String, Double> apply(KV<String, Car> input) {
                            return KV.of(input.getKey(), input.getValue().getPrice());
                        }
                    }));
        }
    }
}
