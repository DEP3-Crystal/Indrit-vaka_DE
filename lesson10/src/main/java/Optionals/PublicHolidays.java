package Optionals;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import swagger.ApiClient;
import swagger.ApiException;
import swagger.api.PublicHolidayApi;
import swagger.model.PublicHolidayV3Dto;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class PublicHolidays {
    @Builder(toBuilder = true)
    @Data
    static class CountryInfo {
        private String name;
        private String code;
        private int numberOfHolidays;
    }

    public static void main(String[] args) throws IOException {
        InputStream inputStream = PublicHolidays.class.getResourceAsStream("/supported.csv");

        List<CountryInfo> countryInfoList;
        try (CSVParser csvParser = CSVParser.parse(new InputStreamReader(inputStream, StandardCharsets.UTF_8),
                CSVFormat.Builder.create()
                        .setHeader("country", "code")
                        .setSkipHeaderRecord(true)
                        .build())) {

            countryInfoList = csvParser.stream()
                    .parallel()
                    .map(v1 -> CountryInfo.builder()
                            .name(v1.get("country"))
                            .code(v1.get("code"))
                            .build()).collect(Collectors.toList());
        }
        System.out.println(countryInfoList);
        ApiClient client = new ApiClient();
        client.setBasePath("https://date.nager.at");
        PublicHolidayApi api = new PublicHolidayApi(client);


        Function<CountryInfo, Stream<CountryInfo>> flatMapper =
                c -> getCountryHolidays(api, c).stream();

        Function<CountryInfo, Stream<CountryInfo>> flatMapper2 =
                c -> setHolidays.apply(api, c).stream();
        long toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();

        var phPerCountry = countryInfoList.stream()
                .parallel()
                .flatMap(flatMapper2)
                .collect(Collectors.toMap(CountryInfo::getName, CountryInfo::getNumberOfHolidays));


        Map.Entry<String, Integer> countryWithMaxHolidays =
                phPerCountry.entrySet().stream().max(Comparator.comparingInt(Map.Entry::getValue)).orElseThrow();

        System.out.println("country with most holidays: " + countryWithMaxHolidays);

        Map.Entry<String, Integer> countryWithLessPH =
                phPerCountry.entrySet()
                        .stream()
                        .min(Comparator.comparingInt(Map.Entry::getValue)).orElseThrow();
        System.out.println("Country with less PH is: " + countryWithLessPH);
        System.out.println("It took: " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");
    }

    static BiFunction<PublicHolidayApi, CountryInfo, Optional<CountryInfo>> setHolidays = (api, c) -> {
        try {
            List<PublicHolidayV3Dto> ph = api.publicHolidayPublicHolidaysV3(2022, c.getCode());
            if (ph != null) {
                c.setNumberOfHolidays(ph.size());
                return Optional.of(c);
            } else {
                return Optional.empty();
            }
        } catch (ApiException e) {
            System.out.println(c.name);

            throw new RuntimeException(e);
        }
    };

    public static Optional<CountryInfo> getCountryHolidays(PublicHolidayApi api, CountryInfo c) {
        try {
            List<PublicHolidayV3Dto> ph = api.publicHolidayPublicHolidaysV3(2022, c.getCode());
            if (ph != null) {
                c.setNumberOfHolidays(ph.size());
                return Optional.of(c);
            } else {
                return Optional.empty();
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }

}

