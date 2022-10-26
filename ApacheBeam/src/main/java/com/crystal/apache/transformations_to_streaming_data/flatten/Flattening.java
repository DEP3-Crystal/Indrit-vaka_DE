package com.crystal.apache.transformations_to_streaming_data.flatten;

import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.group_by.PrintResultFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Flattening {
    public static void main(String[] args) {
        String root = "ApacheBeam/src/main/resources/source/car_ads";
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> car_ads_1 = pipeline.apply(TextIO.read().from(root + "/car_ads_1.csv"));
        PCollection<String> car_ads_2 = pipeline.apply(TextIO.read().from(root + "/car_ads_2.csv"));
        PCollection<String> car_ads_3 = pipeline.apply(TextIO.read().from(root + "/car_ads_3.csv"));
        PCollectionList<String> pCollectionList = PCollectionList.of(car_ads_1).and(car_ads_2).and(car_ads_3);
        PCollection<String> flattenedCollection = pCollectionList.apply(Flatten.pCollections());
        String header = "car,price,body,mileage,engV,engType,registration,year,model,drive";
        flattenedCollection
                .apply("Removing headers", ParDo.of(new RemoveHeadersFn(header)))
                .apply("Deserializing object", ParDo.of(new DeserializeCarFn()))
                .apply("Make car KV", ParDo.of(new ToKVByEntryFn<>("make")))
                .apply("Counting", Count.perKey())
                .apply(" Printing result", ParDo.of(new PrintResultFn<>()));
        pipeline.run().waitUntilFinish();

    }
}
