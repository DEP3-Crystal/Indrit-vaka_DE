package com.crystal.apache.transformations_to_streaming_data.group_by;

import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.model.MallInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class Joining {
    public static void main(String[] args) throws InterruptedException {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        String csvInfoHeader = "CustomerID,Gender,Age,Annual_Income";
        var mallInfoKV = pipeline.apply(TextIO.read().from(root + "source/mall/mall_customers_info.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeadersFn(csvInfoHeader)))
                .apply("deserialization", ParDo.of(new DeserializeMallInfo()))
                .apply("IdIncomeKV", ParDo.of(new ToKVByEntryFn<>("customerID")));

        mallInfoKV.apply("Printing to console", ParDo.of(new DoFn<KV<String, MallInfo>, Void>() {
            @ProcessElement
            public void printElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        pipeline.run().waitUntilFinish();

    }

    private static class DeserializeMallInfo extends DoFn<String, MallInfo> {
        @ProcessElement
        public void deserialize(ProcessContext context) {
            //CustomerID,Gender,Age,Annual_Income
            String[] entries = context.element().split(",");
            int customerId = Integer.parseInt(entries[0]);
            String gender = entries[1];
            int age = Integer.parseInt(entries[2]);
            double annualIncome = Double.parseDouble(entries[3]);

            MallInfo mallInfo = MallInfo.builder()
                    .customerID(customerId)
                    .gender(gender)
                    .age(age)
                    .annualIncome(annualIncome)
                    .build();
            context.output(mallInfo);
        }
    }
}
