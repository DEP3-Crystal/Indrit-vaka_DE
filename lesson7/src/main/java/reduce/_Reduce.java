package reduce;

import jdk.dynalink.linker.ConversionComparator;
import reduce.model.City;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class _Reduce {
    public static void main(String[] args) {
        Set<City> cities = populateSet();

        System.out.println(cities.size());

        //reduce
        City cityWithHighestPopulation = cities.stream()
                .reduce((v1, v2) -> v1.getPeople() > v2.getPeople() ? v1 : v2)
                .orElseThrow();
        System.out.println("City with highest population: "
                + cityWithHighestPopulation.getName()
                + "(" + cityWithHighestPopulation.getPeople() + ")"
        );

        //Grouping by
        Map<String, List<City>> citiesPerState = cities.stream()
                .collect(Collectors.groupingBy(City::getState));
        List<City> new_york = citiesPerState.get("New York");
        System.out.println("Total Cities in New York: " + new_york.size());
        System.out.println("Cities of New York");
        new_york.forEach(city -> System.out.println("\t" + city.getName()));


        Map<String, Long> cityNumberPerState = cities.stream()
                .collect(Collectors.groupingBy(City::getState, Collectors.counting()));
        Map.Entry<String, Long> stateWithMostCities = cityNumberPerState.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow();
        System.out.println("State with most cities: " + stateWithMostCities);

        //State with the most people
        Map<String, Integer> peoplePerCity = cities.stream()
                .collect(Collectors.groupingBy(City::getState, Collectors.summingInt(City::getPeople)));

        Map.Entry<String, Integer> stateWithMostPeople = peoplePerCity.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow();
        System.out.println("State with most people: " + stateWithMostPeople);

    }

    private static Set<City> populateSet() {
        Set<City> cities = null;
        Path path = Path.of("lesson7/src/main/resources/cities.csv");

        try (Stream<String> lines = Files.lines(path, StandardCharsets.ISO_8859_1);) {
            cities = lines.skip(2)
                    .map(line -> {
                        String[] parts = line.split(";");
                        City city = new City();
                        city.setName(parts[1]);
                        city.setState(parts[2]);
                        String peopleAsString = parts[3].replace(" ", "");
                        city.setPeople(Integer.parseInt(peopleAsString));
                        String areaAsString = parts[4].replace(" ", "").replace(',', '.');
                        city.setArea(Double.parseDouble(areaAsString));
                        return city;
                    }).collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cities;
    }
}
