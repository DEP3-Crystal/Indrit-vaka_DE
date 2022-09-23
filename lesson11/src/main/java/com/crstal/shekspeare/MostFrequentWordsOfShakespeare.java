package com.crstal.shekspeare;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MostFrequentWordsOfShakespeare {
    public static void main(String[] args) {
        // stream all words from shakespeare operas - what are the most frequent 15 words
        // 30_000 words - 30_000 map size
        long toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();

        try (Stream<String> lines = Files.lines(Path.of("lesson11/src/main/resources/words.shakespeare.txt"))) {
            lines
                    .map(String::toLowerCase)
                    .collect(Collectors.groupingBy(s -> s, Collectors.counting()))
                    .entrySet()
                    .stream()
                    .sorted((o1, o2) -> (int) (o2.getValue() - o1.getValue()))
                    .limit(15)
                    .forEach(System.out::println);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Took " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");

    }
}
