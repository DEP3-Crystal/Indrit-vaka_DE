package word_count;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/words/words.txt"))
                .apply("ExtractedWord", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via(line -> Arrays.asList(line.toLowerCase().split(" "))))

                .apply("CountWords", Count.perElement())

                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((wordCount) ->
                                wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("src/main/resources/sink/word_count"));

        pipeline.run();
        System.out.println("finished");
    }
}
