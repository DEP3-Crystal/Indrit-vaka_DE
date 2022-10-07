package file_source;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class TotalScoreComputationWithPipelineOptions {
    private static final String CSV_HEADER = "ID,Name,Physics,Chemistry,Math,English,Biology,History";

    public  interface  TotalScoreComputationOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/student_scores.csv")
        String getInputFile();
        void setInputFile(String value);
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }
    public static void main(String[] args) {
        TotalScoreComputationOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(TotalScoreComputationOptions.class);

        System.out.println("****Input file: " + options.getInputFile());
        System.out.println("****Output file: " + options.getOutputFile());

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(TextIO.write().to(options.getOutputFile())
                        .withHeader("Name,Total")
                       // .withNumShards(1) //limiting the output file numbers (also we lose some parallelism)
                );

        pipeline.run();
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

    private static class ComputeTotalScoreFn extends DoFn<String, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] entry = c.element().split(",");
            String name = entry[1];
            Integer totalScore = Integer.parseInt(entry[2]) + Integer.parseInt(entry[3])
                    + Integer.parseInt(entry[4]) + Integer.parseInt(entry[5])
                    + Integer.parseInt(entry[6]) + Integer.parseInt(entry[7]);

            c.output(KV.of(name, totalScore));
        }
    }

    private static class ConvertToStringFn extends DoFn<KV<String, Integer>, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getKey() + "," + c.element().getValue());
        }
    }
}
