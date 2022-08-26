package reduce;

import reduce.model.City;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class _Reduce {
    public static void main(String[] args) {
        Set<City> cities = populateSet();

        System.out.println(cities.size());

        Map<String, List<City>> citiesPerState = cities.stream()
                .collect(Collectors.groupingBy(City::getState));
        List<City> new_york = citiesPerState.get("New York");
        System.out.println("Total Cities in New York: " + new_york.size());
        System.out.println("Cities of New York");
        new_york.forEach( city -> System.out.println("\t"+city.getName()));
    }

    private static Set<City> populateSet(){
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
