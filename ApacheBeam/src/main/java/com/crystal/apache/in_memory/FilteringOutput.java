package com.crystal.apache.in_memory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class FilteringOutput {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        List<Double> data = Arrays.asList(65191.0, 981.0, 81.0, .8819, 158.415);

        pipeline.apply(Create.of(data))
                .apply(MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("-pre-filtered: " + input);
                        return input;
                    }
                }))
                .apply(ParDo.of(new Filtering.FilterThresholdFn(100)))
                .apply(
//                        MapElements.via(new SimpleFunction<Double, Double>() {
//                    @Override
//                    public Double apply(Double input) {
//                        System.out.println("-filtered: " + input);
//                        return input;
//                    }
//                })
                        "filtered", MapElements
                                .into(TypeDescriptors.voids())
                                .via(input -> {
                                    System.out.println("-filtered: " + input);
                                    return null;
                                }));

        pipeline.run();

    }
}
