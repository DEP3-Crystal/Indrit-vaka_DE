package com.crystal.apache.schema;

import com.crystal.apache.schema.model.SalesRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class SchemaDefinitionFromFile {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<SalesRecord> salesRecord = readSalesRecord(pipeline);

        System.out.println("has schema? " + salesRecord.hasSchema());
        System.out.println("schema: " + salesRecord.getSchema());

        System.out.println("Processing pipeline finished");

        salesRecord.apply(MapElements
                .via(new SimpleFunction<SalesRecord, Void>() {
                    @Override
                    public Void apply(SalesRecord input) {
                        System.out.println(input);
                        return null;
                    }
                }));

        pipeline.run().waitUntilFinish();

    }

    private static PCollection<SalesRecord> readSalesRecord(Pipeline pipeline) {

        var record1 = new SalesRecord(
                "1/5/09 5:39", "Shoes", 120f, "Amex", "Netherlands");
        var record2 = new SalesRecord(
                "2/2/09 9:16", "Jeans", 110f, "Mastercard", "United States");
        var record3 = new SalesRecord(
                "3/5/09 10:08", "Pens", 10f, "Visa", "United States");
        var record4 = new SalesRecord(
                "4/2/09 14:18", "Shoes", 303f, "Visa", "United States");
        var record5 = new SalesRecord(
                "5/4/09 1:05", "iPhone", 1240f, "Diners", "Ireland");
        var record6 = new SalesRecord(
                "6/5/09 11:37", "TV", 1503f, "Visa", "Canada");

        return pipeline.apply(Create.of(record1,record2,record3,record4,record5,record6));
    }
}
