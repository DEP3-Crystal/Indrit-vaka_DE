package com.crystal.apache.transformations_to_streaming_data.group_by;


import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFnOld;
import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GroupingV2 {
    private static final String HEADER = "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        pipeline.apply("Reading data", TextIO.read().from(root + "source/car_ads/car_ads_*.csv"))
                .apply("Filtering header", ParDo.of(new RemoveHeadersFn(HEADER))) //TODO to implement using State information
                .apply("Transforming into Car objects", ParDo.of(new DeserializeCarFn()))
//                .apply("windowing", Window.)
                .apply("Grouping by ", ParDo.of(new ToKVByEntryFnOld<>(HEADER, "car")))
                .apply("MakeModelGrouping", GroupByKey.create())
                .apply("Compute Average for each make of car", ParDo.of(new ComputeAveragePriceFn()))
                .apply("printing avg price per make", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void print(ProcessContext c){
                        //TODO use logger
                        System.out.println(c.element());
                    }
                }))

        ;

        pipeline.run().waitUntilFinish();

    }

    private static class ComputeAveragePriceFn extends DoFn<KV<String, Iterable<Car>>, KV<String, Double>> {
        @ProcessElement
        public void getAveragePrice(ProcessContext c){
            KV<String, Iterable<Car>> carKV = c.element();
            Double average = StreamSupport.stream(carKV.getValue().spliterator(), false)
                    .collect(Collectors.averagingDouble(Car::getPrice));
            c.output(KV.of(carKV.getKey(), average));
        }
    }
}
