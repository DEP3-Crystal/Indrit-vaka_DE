package com.crystal.apache.transformations_to_streaming_data.par_do.compution;

import com.crystal.apache.transformations_to_streaming_data.core.DeserializeCarFn;
import com.crystal.apache.transformations_to_streaming_data.core.RemoveHeadersFn;
import com.crystal.apache.transformations_to_streaming_data.model.Car;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import static com.crystal.apache.transformations_to_streaming_data.par_do.compution.OldestCar.getCarAge;

public class NewestCar {
    private static final String HEADER = "car,price,body,mileage,engV,engType,registration,year,model,drive";


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        String root = "ApacheBeam/src/main/resources/";
        pipeline.apply("Reading data", TextIO.read().from(root + "source/car_ads/car_ads_*.csv"))
                .apply("Filtering header", ParDo.of(new RemoveHeadersFn(HEADER)))
                .apply("Deserializing", ParDo.of(new DeserializeCarFn()))
                .apply("Oldest car", Min.globally((o1, o2) -> getCarAge(o1) - getCarAge(o2)
                ))
                .apply("Printing oldest car", MapElements
                        .via(new SimpleFunction<Car, Void>() {
                            @Override
                            public Void apply(Car car) {
                                System.out.println(HEADER);
                                System.out.println(car);
                                System.out.println(getCarAge(car) + " years old");
                                return null;
                            }
                        }))
        ;
        pipeline.run().waitUntilFinish();

    }


}


