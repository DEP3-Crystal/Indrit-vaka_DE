package com.training.exercises;

import java.util.stream.Stream;

public class StreamOfChar {
    static final String VOCALS = "aeiou";

    static String myText = "There also could occur errors during search or poison application Or the search could take a very long time because the ants hide very well";

    // TO DO - find 2 other ways to create a stream of chars from a string !!!!
    public static void main(String[] args) {
        Stream<Character> characterStream = new String(myText).chars().mapToObj(i -> (char) i);
    }
}
