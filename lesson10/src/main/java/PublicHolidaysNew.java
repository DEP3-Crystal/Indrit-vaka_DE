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
import java.util.stream.Collectors;

public class PublicHolidaysNew {

    @Builder(toBuilder = true)
    @Data
    static class CountryInfo {
        private String name;
        private String code;
        private int numberOfHolidays;
    }


    public static void main(String[] args) throws IOException {
//        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "1");
        InputStream inputStream = PublicHolidays.class.getResourceAsStream("/supported.csv");
        ApiClient client = new ApiClient();
        client.setBasePath("https://date.nager.at");
        PublicHolidayApi publicHolidayApi = new PublicHolidayApi(client);
        long toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();
        Map<String, Integer> phPerCountry;
        try (CSVParser csvParser = CSVParser.parse(new InputStreamReader(inputStream, StandardCharsets.UTF_8), CSVFormat.DEFAULT)) {
                phPerCountry = csvParser.stream()
                        .map(v1 -> CountryInfo.builder()
                                .name(v1.get(0))
                                .code(v1.get(1))
                                .build())
                        .toList().stream()
                        .parallel()
                        .map(c -> {
                            try {
                                List<PublicHolidayV3Dto> ph = publicHolidayApi.publicHolidayPublicHolidaysV3(2022, c.getCode());
                                c.setNumberOfHolidays(ph.size());
                                return c;
                            } catch (ApiException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .peek(s-> System.out.println(s+" " +Thread.currentThread().getName()))
                        .collect(Collectors.toMap(c -> c.name, c -> c.numberOfHolidays));

        }

        Map.Entry<String, Integer> countryWithMaxHolidays = phPerCountry.entrySet().stream().max(Comparator.comparingInt(Map.Entry::getValue)).orElseThrow();

        System.out.println("country with most holidays: " + countryWithMaxHolidays);

        Map.Entry<String, Integer> countryWithLessPH = phPerCountry.entrySet().stream().min(Comparator.comparingInt(Map.Entry::getValue)).orElseThrow();
        System.out.println("Country with less PH is: " + countryWithLessPH);
        System.out.println("It took: " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");


    }

}
