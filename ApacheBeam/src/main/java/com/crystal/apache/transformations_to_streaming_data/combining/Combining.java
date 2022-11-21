package com.crystal.apache.transformations_to_streaming_data.combining;

import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.group_by.Joining;
import com.crystal.apache.transformations_to_streaming_data.group_by.PrintResultFn;
import com.crystal.apache.transformations_to_streaming_data.model.MallCustomerInfo;
import com.crystal.apache.transformations_to_streaming_data.model.MallCustomerScore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Combining {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        String csvInfoHeader = "CustomerID,Gender,Age,Annual_Income";
        String csvScoreHeader = "CustomerID,Spending Score";
        pipeline.apply(TextIO.read().from(root + "source/mall/mall_customers_info.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeadersFn(csvInfoHeader)))
                .apply("deserialization", ParDo.of(new Joining.DeserializeMallInfo()))
                .apply("Extracting age", ParDo.of(new DoFn<MallCustomerInfo, Double>() {
                    @ProcessElement
                    public void processElement(ProcessContext context){
                        context.output((double) context.element().getAge());
                    }
                }))
                .apply("Age average", Combine.globally(new AverageFn()))
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