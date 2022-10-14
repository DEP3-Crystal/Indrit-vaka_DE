import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> letters1 = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));
        PCollection<String> letters2 = pipeline.apply(Create.of(Arrays.asList("d", "e", "f")));
        PCollection<String> letters3 = pipeline.apply(Create.of(Arrays.asList("g", "h", "i")));


        PCollectionList<String> allLetters = PCollectionList.of(letters1).and(letters2).and(letters3);
        List<PCollection<String>> lettersCollections = allLetters.getAll();
        // Try Flatten transform - > "a"..."i" !!!! TO YOU - Try Flatten -


        //PCollection<PCollection<String>>
//    PCollection<PCollectionList<String>> apply = pipeline1.apply(Create.of(allLetters));
        allLetters.apply(Flatten.pCollections())
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void printEl(ProcessContext c){
                        System.out.println(c.element());
                        c.output(c.element());
                    }
                }));
        pipeline.run();
    }
}
