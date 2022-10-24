package com.crystal.apache.transformations_to_streaming_data.combining;

import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.group_by.Joining;
import com.crystal.apache.transformations_to_streaming_data.group_by.PrintResultFn;
import com.crystal.apache.transformations_to_streaming_data.model.MallCustomerInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CombiningPerKey {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        String csvInfoHeader = "CustomerID,Gender,Age,Annual_Income";
        pipeline.apply(TextIO.read().from(root + "source/mall/mall_customers_info.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeadersFn(csvInfoHeader)))
                .apply("deserialization", ParDo.of(new Joining.DeserializeMallInfo()))
                .apply("Extracting age", ParDo.of(new ToKVByEntryFn<>("gender")))
                .apply("Extracting age", ParDo.of(new DoFn<KV<String, MallCustomerInfo>, KV<String, Double>>() {
                    @ProcessElement
                    public void getAge(ProcessContext c)
                    {
                        c.output(KV.of(c.element().getKey(), (double)c.element().getValue().getAge()));
                    }
                }))
                .apply("Age average", Combine.perKey(new AverageFn()))
                .apply("Printing average", ParDo.of(new PrintResultFn<>()) );

        pipeline.run().waitUntilFinish();
    }
    public static class AverageFn implements SerializableFunction<Iterable<Double>, Double> {
        @Override
        public Double apply(Iterable<Double> input) {
            return StreamSupport.stream(input.spliterator(), false).collect(Collectors.averagingDouble(value -> value));
        }
    }
}
