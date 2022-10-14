package com.crystal.apache.payment_type_processing.streaming;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PaymentTypeProcessingWithCostumeOptions {
    private static String CSV_HEADER = "Date,Product,Card,Country";

    public static void main(String[] args) {

        //Execute from terminal
        //mvn compile exec:java -D"exec.mainClass=com.crystal.apache.payment_type_processing.streaming.PaymentTypeProcessingWithCostumeOptions" -D"exec.args='--outputFile=src/main/resources/sink/payment_processing/payment_type_count'"

        AveragePriceProcessingOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read()
                        .from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeAveragePriceFn()))
                .apply("AvrageAggregation", Mean.perKey())
                .apply("Foramt result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(productKV -> productKV.getKey()+"," + productKV.getValue()))
                .apply("Writing results", TextIO.write()
                        .to(options.getOutputFile())
                        //.withNumShards(2)
                       // .withoutSharding()
                        .withShardNameTemplate("-SSS")
                        .withHeader("productName,avgPrice")
                )

        ;
        pipeline.run();
        System.out.println("Processing finished");
    }

    public interface AveragePriceProcessingOptions extends PipelineOptions {
        @Description("Path to the file to read from")
        @Default.String("src/main/resources/source/SalesJan2009.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }

    private static class FilterHeaderFn extends DoFn<String, String> {
        private String header;

        public FilterHeaderFn(String csvHeader) {

            header = csvHeader;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();
            if (!row.isEmpty() && !row.contains(header)) {
                c.output(row);
            }
        }
    }

    private static class ComputeAveragePriceFn extends DoFn<String, KV<String ,Double>> {

        @ProcessElement
        public void progressElement(ProcessContext c){
            String[] entries = c.element().split(",");
            String productName = entries[1];
            Double price = Double.parseDouble(entries[2]);

            c.output(KV.of(productName, price));
        }
    }
}
