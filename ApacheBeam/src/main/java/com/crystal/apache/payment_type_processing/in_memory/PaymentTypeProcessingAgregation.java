package com.crystal.apache.payment_type_processing.in_memory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaymentTypeProcessingAgregation {

    public static void main(String[] args) {
        //executing from terminal
        //mvn compile exec:java -D"exec.mainClass=com.crystal.apache.in_memory.payment.ExtractingPaymentTypeUsingMapAndFlatMap"
        final List<String> LINES = Arrays.asList(
                "1/5/09 5:29,Shoes,1200,Amex,Netherlands",
                "1/2/09 9:16,Jacket,2500,Mastercard,US",
                "1/2/09 10:16,Phone,3500,Visa,Canada",
                "1/2/09 10:30,Shoes,2500,Visa,Canada",
                "1/2/09 11:30,Phone,1500,Diners,Ireland",
                "1/2/09 11:30,Books,1100,Visa,Ireland"
        );

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(LINES))
                .apply("printing records", MapElements.via(new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        System.out.println(input);
                        return input;
                    }
                }))
                .apply("ExtractPaymentTypes", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via(line -> Collections.singleton(line.split(",")[3])))
                .apply("CountAgregation", Count.perElement())
                .apply("Format result",MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount ->
                                typeCount.getKey() + ":" + typeCount.getValue())
                )
                .apply(MapElements.via(new SimpleFunction<String, Void>() {
                    @Override
                    public Void apply(String input) {
                        System.out.println(input);
                        return null;
                    }
                }))
        ;

        pipeline.run().waitUntilFinish();
        System.out.println("Pipeline execution completed!");
    }


}
