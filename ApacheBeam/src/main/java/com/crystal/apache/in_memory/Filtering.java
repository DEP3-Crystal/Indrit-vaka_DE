package com.crystal.apache.in_memory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.List;

public class Filtering {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        List<Double> data = Arrays.asList(65191.0, 981.0, 81.0, 0.8819, 158.415);
        pipeline.apply(Create.of(data))
                .apply(ParDo.of(new FilterThresholdFn(82)));
        pipeline.run();

    }

    public static class FilterThresholdFn extends DoFn<Double, Double> {
        private double threshold;

        public FilterThresholdFn(double threshold) {
            this.threshold = threshold;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element() > threshold) {
                c.output(c.element());
            }
        }
    }
}
