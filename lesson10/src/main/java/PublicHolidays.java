import lombok.Builder;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import swagger.ApiClient;
import swagger.ApiException;
import swagger.api.PublicHolidayApi;
import swagger.model.PublicHolidayV3Dto;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PublicHolidays {
    @Builder(toBuilder = true)
    @Data
    static class CountryInfo {
        private String name;
        private String code;
        private int numberOfHolidays;
    }

    public static void main(String[] args) throws IOException, ApiException {
        long toEpochMilli = ZonedDateTime.now().toInstant().toEpochMilli();
        InputStream inputStream = PublicHolidays.class.getResourceAsStream("/supported.csv");

        List<CountryInfo> countryInfoList;
        try (CSVParser csvParser = CSVParser.parse(new InputStreamReader(inputStream, StandardCharsets.UTF_8), CSVFormat.DEFAULT)) {
            countryInfoList =
                    csvParser.stream()
                            .map(v1 -> CountryInfo.builder()
                                    .name(v1.get(0))
                                    .code(v1.get(1))
                                    .build()).toList();
            System.out.println(countryInfoList.size());
//           countryInfoList.forEach(System.out::println);
        }

        ApiClient client = new ApiClient();
        client.setBasePath("https://date.nager.at");
        PublicHolidayApi publicHolidayApi = new PublicHolidayApi(client);

        List<PublicHolidayV3Dto> alHolidays = publicHolidayApi.publicHolidayPublicHolidaysV3(2022, "al");

        System.out.println(alHolidays.size());
        Map<String, Integer> phPerCountry = countryInfoList.stream()
                .parallel()
                .map(countryInfo -> {
                    try {
                        var ph = publicHolidayApi.publicHolidayPublicHolidaysV3(2022, countryInfo.getCode());
                        countryInfo.setNumberOfHolidays(ph.size());
                        return countryInfo;
                    } catch (ApiException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toMap(c -> c.name, c -> c.numberOfHolidays));

        Map.Entry<String, Integer> countryWithMaxHolidays =
                phPerCountry.entrySet()
                        .stream()
                        .max(Comparator.comparingInt(Map.Entry::getValue))
                        .orElseThrow();

        System.out.println("country with most holidays: " + countryWithMaxHolidays);

        Map.Entry<String, Integer> countryWithLessPH = phPerCountry.entrySet()
                .stream()
                .min(Comparator.comparingInt(Map.Entry::getValue))
                .orElseThrow();

        System.out.println("Country with less PH is: " + countryWithLessPH);
        System.out.println("It took: " + (ZonedDateTime.now().toInstant().toEpochMilli() - toEpochMilli) + "ms");

    }

    //                        //    Getting supported and nonSupported codes
//                        if (ph != null) {
//                            System.out.println(c.name + " " + ph.size());
//                            saveFile("lesson10/src/main/resources/supported.csv", c);
//                        } else {
//                            saveFile("lesson10/src/main/resources/notSupported.csv", c);
//                        }
    private static void saveFile(String fileName, CountryInfo c) {

        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(fileName, true), CSVFormat.DEFAULT)) {
            csvPrinter.printRecord(c.name, c.code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
