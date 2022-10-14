package com.crystal.apache.payment_type_processing.in_memory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.List;

public class ExtractingPaymentType {

    public static void main(String[] args) {
        //executing from terminal
        //mvn compile exec:java -D"exec.mainClass=com.crystal.apache.in_memory.payment.ExtractingPaymentType"
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
                .apply("printing records",ParDo.of(new PrintToConsoleFn()))
                .apply(ParDo.of(new ExtractPaymentTypeInfoFn()))
                .apply("printing payment type",ParDo.of(new PrintToConsoleFn()))
        ;

        pipeline.run();
        System.out.println("Pipeline execution completed!");
    }

    private static class PrintToConsoleFn extends DoFn<String, String> {
        @ProcessElement
        public void printElement(ProcessContext context) {
            System.out.println(context.element());
            context.output(context.element());
        }
    }

    private static class ExtractPaymentTypeInfoFn extends DoFn<String, String> {
        @ProcessElement
        public void extractPaymentType(ProcessContext c) {
            String[] entries = c.element().split(",");
            if (entries.length >= 4)
                c.output(entries[3]);
        }
    }
}
