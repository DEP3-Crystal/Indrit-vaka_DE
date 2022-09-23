package com.crstal.shekspeare;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MostUsedWord {
    public static void main(String[] args) {
        Function<String, Stream<String>> convertToWord = line -> Arrays.stream(line.split(" "));

        try(Stream<String> lines = Files.lines(Path.of("lesson11/src/main/resources/shakespeare.txt"))){

            lines.flatMap(convertToWord)
                    .filter(s -> s.length() >1) // remove stop words
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
    }
}
