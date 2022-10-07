package crystal.training.exercises;


import crystal.training.Person;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FuncProg2 {
    static Integer[] values = new Integer[]{1, 2, 3, 4, 5};

    static Collection<Integer> printProcessedValues(Integer[] values, Function<Integer, Integer> converter) {
        return Arrays.stream(values).map(converter::apply).filter(v -> v < 10).collect(Collectors.toList());
    }

    public static List<Person> processPersons(List<Person> people, Predicate<Person> condition) {
        return people.stream().filter(condition).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        System.out.println(printProcessedValues(values, v -> v * 3));
        // TBD - filter also functional argument Function<Integer,Boolean>
        Arrays.stream(new Integer[]{1, 2}).map(v ->
        {
            System.out.println("VALUE IS " + v);
            return null;
        });
        // WHY System.out.println("VALUE IS " + v); not executed ??

        //Because we haven't collected the data => .collect(Collectors.toList())

        //Arrays.stream(new Integer[]{1, 2}).map(v ->
        //{
        //    System.out.println("VALUE IS " + v);
        //    return null;
        //}).collect(Collectors.toList());


        // 1 create result collection ✅
        // 2 iterate over input collection (stream) ✅
        // 3 for each element calculate and save result into result collection ✅
        // 3.1 filter out on condition  ✅
        // 4 return result collection ✅

        List<Person> personList = List.of(
                new Person("Indrit", 21, "indrit.vaka@crystal-system.eu"),
                new Person("Bob", 17, "bob@ascv.cm"),
                new Person("Alex", 22, "adssd@ascv.cm"),
                new Person("ads", 15, "bob@ascv.cm"));

        //Here we collect the result and store it in a var by filtering the values.
        //          Here we are filtering the person based on his age
        List<Person> adult = processPersons(personList, p -> p.getAge() >= 18);
        adult.forEach(person -> System.out.println(person.getName()));

    }
}

