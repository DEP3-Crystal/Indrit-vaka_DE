package com.crystal.apache.fixed_window;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.time.format.DateTimeFormatter;

public class FixedWindow {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> carMakesTimes = pipeline.apply(Create.timestamped(
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:05").toInstant()),
                TimestampedValue.of("Tesla", new DateTime("2020-12-12T20:30:06").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:07").toInstant()),
                TimestampedValue.of("Tesla", new DateTime("2020-12-12T20:30:08").toInstant()),
                TimestampedValue.of("Tesla", new DateTime("2020-12-12T20:30:11").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:11").toInstant()),
                TimestampedValue.of("Tesla", new DateTime("2020-12-12T20:30:12").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:13").toInstant()),
                TimestampedValue.of("Tesla", new DateTime("2020-12-12T20:30:14").toInstant()),
                TimestampedValue.of("Ford", new DateTime("2020-12-12T20:30:16").toInstant()),
                TimestampedValue.of("Tesla", new DateTime("2020-12-12T20:30:17").toInstant())));
        PCollection<String> fixedWindow = carMakesTimes.apply(
                "Window", Window.into(FixedWindows.of(Duration.standardSeconds(5))));
        PCollection<KV<String, Long>> carTypeCount = fixedWindow.apply(Count.perElement());

        carTypeCount.apply(ParDo.of(new DoFn<KV<String, Long>, String>() {
                    @ProcessElement
                    public void toString(ProcessContext c) {
                        c.output(String.format("%s %s", c.element().getKey(), c.element().getValue()));
                    }
                })).apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void printCar(ProcessContext c) {
                        System.out.println(c.element());
                        c.output(c.element());
                    }
                }))
                .apply(TextIO.write().to("ApacheBeam/src/main/resources/sink/fixedWindow"));


        pipeline.run().waitUntilFinish();

    }
}
