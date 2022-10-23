package com.crystal.apache.transformations_to_streaming_data.group_by;

import com.crystal.apache.transformations_to_streaming_data.core.CarToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class Grouping {
    private static final String HEADER = "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        String root = "ApacheBeam/src/main/resources/";
        pipeline.apply("Reading data", TextIO.read().from(root + "source/car_ads/car_ads_*.csv"))
                .apply("Filtering header", ParDo.of(new RemoveHeadersFn(HEADER)))
                .apply("Transforming into Car objects", ParDo.of(new DeserializeCarFn()))
                .apply("Grouping by ", ParDo.of(new CarToKVByEntryFn(HEADER, "car")))
                .apply("MakeModelGrouping", GroupByKey.create())
                .apply("Printing results", ParDo.of(new DoFn<KV<String, Iterable<Car>>, Void>() {
                    @ProcessElement
                    public void printCars(ProcessContext c){
                        System.out.println(c.element());
                    }
                }))

        ;

        pipeline.run().waitUntilFinish();

    }

}
