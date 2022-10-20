package com.crystal.apache.schema;

import com.crystal.apache.schema.model.SalesRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

public class SchemaDefinitionFromFileV2 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        System.out.println("Starting processing the data");
        pipeline.apply("ReadLines", TextIO.read()
                        .from("ApacheBeam/src/main/resources/source/SalesJan2009.csv"))
                .apply("deserialization", ParDo.of(new DeserializeSaleRecord()))
                .apply(Select.fieldNames("product", "price"))
                .apply("Format result", MapElements
                        .into(TypeDescriptors.strings())
                        .via((Row row) -> row.getString("product")
                                + "," + row.getFloat("price")))
                .apply("WriteResult", TextIO.write()
                        .to("ApacheBeam/src/main/resources/sink/schema/salesRecord")
                        .withSuffix(".csv")
                        .withShardNameTemplate("-SSS")
                        .withHeader("product,price")
                );
        pipeline.run();//.waitUntilFinish();
        System.out.println("Processed!");
    }

    private static class DeserializeSaleRecord extends DoFn<String, SalesRecord> {
        @ProcessElement
        public void deserialize(ProcessContext context) {
            String header = "Date,Product,Price,paymentType,Country";
            if (context.element().contains(header)) return;
            String[] entries = context.element().split(",");
            String date = entries[0];
            String product = entries[1];
            float price = Float.parseFloat(entries[2]);
            String paymentType = entries[3];
            String country = entries[4];
            SalesRecord salesRecord = SalesRecord.builder()
                    .dateTime(date)
                    .product(product)
                    .price(price)
                    .paymentType(paymentType)
                    .country(country)
                    .build();
            context.output(salesRecord);
        }
    }
}
