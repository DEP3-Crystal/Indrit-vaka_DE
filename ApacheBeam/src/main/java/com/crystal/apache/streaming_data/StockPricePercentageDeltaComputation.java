package com.crystal.apache.streaming_data;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

public class StockPricePercentageDeltaComputation {
    private static final String CSV_HEADER =
            "Date,Open,High,Low,Close,Adj Close,Volume,Name";

    public static void main(String[] args) {
        ComputationOptions options = PipelineOptionsFactory.fromArgs(args)
                .as(ComputationOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from(options.getInputFile())
                        .watchForNewFiles(Duration.standardSeconds(10),
                                Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(30))))

                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputePriceDeltaPercentage()))
                .apply(ParDo.of(new DoFn<KV<String, Double>, String>() {
                    @ProcessElement
                    public void convertToString(ProcessContext c) {
                        String date = c.element().getKey();
                        Double percentageRounded = c.element().getValue();
                        c.output(date + "," + percentageRounded);
                    }
                }))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void printElement(ProcessContext c) {
                        System.out.println(c.element());
                        c.output(c.element());
                    }
                    @DoFn.FinishBundle
                    public void finishBundle(DoFn<String, String>.FinishBundleContext finishBundleContext) {
                        System.out.println("-------------------------------------------------");
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

    public interface ComputationOptions extends PipelineOptions {

        @Description("Path of file to read from")
        @Default.String("ApacheBeam/src/main/resources/streaming_source/*.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("ApacheBeam/src/main/resources/sink/percentage_delta")
        String getOutputFile();

        void setOutputFile(String value);
    }

    private static class FilterHeaderFn extends DoFn<String, String> {
        private final String header;

        public FilterHeaderFn(String csvHeader) {

            header = csvHeader;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            var row = c.element();
            if (row.isEmpty() || row.contains(header)) return;
            c.output(row);
        }
    }

    private static class ComputePriceDeltaPercentage extends DoFn<String, KV<String, Double>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            var cols = c.element().split(",");
            String date = cols[0];
            double openPrice = Double.parseDouble(cols[1]);
            double closePrice = Double.parseDouble(cols[4]);
            double percentageDelta = ((closePrice - openPrice) / openPrice) * 100;
            double percentageDeltaRound = Math.round(percentageDelta * 100) / 100.0;
            c.output(KV.of(date, percentageDeltaRound));
        }
    }
}
