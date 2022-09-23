package com.crstal;

import java.time.ZonedDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class PiApprox {

    public static void main(String[] args) {
        int insideCircle = 0;
        int insideRect = 0;
        long toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();

        for (int i = 0; i < 100_000_000; i++) {
            double x = -1 + 2 * Math.random(); // [-1,+1)
            double y = -1 + 2 * Math.random();
            if (x * x + y * y <= 1) {
                insideCircle++;
            }
            insideRect++;
        }

        System.out.println("Approx PI=" + (insideCircle * 4.0) / insideRect);
        System.out.println("Took " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");

        toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();
        // TO DO
        // implement it functional style - streaming/ parallel streaming
        var approximate = 100_000_000;

        var insideCircle1 = IntStream.range(0, approximate)
                .parallel()
                .map(i ->
                {
                    double x = -1 + 2 * ThreadLocalRandom.current().nextDouble(); // [-1,+1)
                    double y = -1 + 2 * ThreadLocalRandom.current().nextDouble();
                    return (x * x + y * y <= 1) ? 1 : 0;
                }).sum();

        System.out.println("Approx PI=" + (insideCircle1 * 4.0) / approximate);
        System.out.println("Took " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");


    }
}
