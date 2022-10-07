package crystal.training.exercises;

import java.util.Arrays;

public class SumOfCharValues {
    static String myText = "The";
    static final String VOCALS = "aeiou";

    public static void main(String[] args) {

        long sum = Arrays.stream(myText.split(""))
                .map(v -> VOCALS.contains(v.charAt(0) + "") ? (int) v.charAt(0) : 0).reduce(Integer::sum).get();

        System.out.println(sum);
    }
}
