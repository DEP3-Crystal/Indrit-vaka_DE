package com.crystal.apache.transformations_to_streaming_data.partitioning;

import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.group_by.PrintResultFn;
import com.crystal.apache.transformations_to_streaming_data.model.Car;
import com.crystal.apache.utils.BeamUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;

public class partitioning {
    public static void main(String[] args) {
        final String header = "car,price,body,mileage,engV,engType,registration,year,model,drive";
        Pipeline pipeline = BeamUtils.createPipeline("Partition");
        PCollectionList<KV<String, Car>> priceCategories = pipeline.apply("Resources", TextIO.read().from("ApacheBeam/src/main/resources/source/car_ads/car_ads_*.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeadersFn(header)))
                .apply("Deserializing to obj", ParDo.of(new DeserializeCarFn()))
                .apply("Make and car", ParDo.of(new ToKVByEntryFn<>("make")))
                .apply(Partition.of(4, new Partition.PartitionFn<KV<String, Car>>() {
                    @Override
                    public int partitionFor(KV<String, Car> elem, int numPartitions) {
                        if (elem.getValue().getPrice() < 2_000)
                            return 0;
                        if (elem.getValue().getPrice() < 5_000)
                            return 1;
                        if (elem.getValue().getPrice() < 10_000)
                            return 2;
                        return 3;
                    }
                }));

        priceCategories.get(2)
                .apply(Count.perKey())
                .apply("number car where price is greater than 10_000", ParDo.of(new PrintResultFn<>()));

        pipeline.run().waitUntilFinish();
    }
}
