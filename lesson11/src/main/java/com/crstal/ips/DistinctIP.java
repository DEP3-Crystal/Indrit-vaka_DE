package com.crstal.ips;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class DistinctIP {

    static int total = 0;

    public static void main(String[] args) {
        // I have a web server - I want to know how many distinct IP's are visiting the site - streaming
        long toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();

        try (Stream<String> lines = Files.lines(Path.of("lesson11/src/main/resources/ip-addresses.txt"))) {
            long distinctIP =
                    lines.peek(i -> total++)
                            .distinct()
                            .count();
            System.out.println("there are " + distinctIP + " distinct ips, total: " + total);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Took " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");

    }
}
