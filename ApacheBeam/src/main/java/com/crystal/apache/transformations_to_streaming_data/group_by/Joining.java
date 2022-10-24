package com.crystal.apache.transformations_to_streaming_data.group_by;

import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.core.ToKVByEntryFn;
import com.crystal.apache.transformations_to_streaming_data.model.Customer;
import com.crystal.apache.transformations_to_streaming_data.model.MallCustomerInfo;
import com.crystal.apache.transformations_to_streaming_data.model.MallCustomerScore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Joining {
    private static final Logger LOG = LoggerFactory.getLogger(Joining.class);
    static final TupleTag<MallCustomerInfo> incomeTag = new TupleTag<>();
    static final TupleTag<MallCustomerScore> scoreTag = new TupleTag<>();

    public static void main(String[] args) throws InterruptedException {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        String csvInfoHeader = "CustomerID,Gender,Age,Annual_Income";
        String csvScoreHeader = "CustomerID,Spending Score";
        var customerIncome = pipeline.apply(TextIO.read().from(root + "source/mall/mall_customers_info.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeadersFn(csvInfoHeader)))
                .apply("deserialization", ParDo.of(new DeserializeMallInfo()))
                .apply("IdIncomeKV", ParDo.of(new ToKVByEntryFn<>("customerID")));
       // customerIncome.apply("Printing result", ParDo.of(new PrintResultFn<>()));

        PCollection<KV<String, MallCustomerScore>> customersScore = pipeline.apply(TextIO.read().from("ApacheBeam/src/main/resources/source/mall/mall_customers_score.csv"))
                .apply("Filtering headers", ParDo.of(new RemoveHeadersFn(csvScoreHeader)))
                .apply("Deserialize object", ParDo.of(new DeserializeMallCustomerScoreFn()))
                .apply("IdIncomeKV", ParDo.of(new ToKVByEntryFn<>("customerID")));

        // customersScore.apply("Printing result", ParDo.of(new PrintResultFn<>()));

        PCollection<KV<String, CoGbkResult>> joined = KeyedPCollectionTuple
                .of(incomeTag, customerIncome)
                .and(scoreTag, customersScore)
                .apply(CoGroupByKey.create());

        joined.apply("Deserialize to customer", ParDo.of(new DeserializeCostumerFn()))
                        .apply("Printing results", ParDo.of(new PrintResultFn<>()));



        pipeline.run().waitUntilFinish();


    }

    public static class DeserializeMallInfo extends DoFn<String, MallCustomerInfo> {
        @ProcessElement
        public void deserialize(ProcessContext context) {
            //CustomerID,Gender,Age,Annual_Income
            String[] entries = context.element().split(",");
            int customerId = Integer.parseInt(entries[0]);
            String gender = entries[1];
            int age = Integer.parseInt(entries[2]);
            double annualIncome = Double.parseDouble(entries[3]);

            MallCustomerInfo mallCustomerInfo = MallCustomerInfo.builder()
                    .customerID(customerId)
                    .gender(gender)
                    .age(age)
                    .annualIncome(annualIncome)
                    .build();
            context.output(mallCustomerInfo);
        }
    }

    private static class DeserializeMallCustomerScoreFn extends DoFn<String, MallCustomerScore> {
        //CustomerID,Spending Score
        @ProcessElement
        public void deserialize(ProcessContext context) {
            String[] entries = context.element().split(",");
            int customerId = Integer.parseInt(entries[0]);
            double spendingScore = Double.parseDouble(entries[1]);
            MallCustomerScore mallCustomerScore = MallCustomerScore.builder()
                    .customerID(customerId)
                    .spendingScore(spendingScore)
                    .build();
            context.output(mallCustomerScore);
        }
    }

    private static class DeserializeCostumerFn extends DoFn<KV<String, CoGbkResult>, Customer> {
        @ProcessElement
        public void deserialize(ProcessContext c){
            KV<String, CoGbkResult> element = c.element();
            MallCustomerInfo info = element.getValue().getOnly(incomeTag);
            MallCustomerScore score = element.getValue().getOnly(scoreTag);
            Customer customer = Customer.builder()
                    .customerID(info.getCustomerID())
                    .age(info.getAge())
                    .annualIncome(info.getAnnualIncome())
                    .gender(info.getGender())
                    .spendingScore(score.getSpendingScore())
                    .build();
            c.output(customer);
        }
    }
}
