package crystal.CRUD;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Insert {
    public static void main(String[] args) throws IOException, InterruptedException {
        String path = "C:\\Users\\indri\\Downloads\\bigquery-368715-8d3525b68fc2.json";
        Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(path));

        BigQuery bigQuery = BigQueryOptions.newBuilder()
                .setProjectId("bigquery-368715")
                .build().getService();
        final String Insert_Person =
                getInsertSQL(new Person(1, "Indrit"), new Person(2, "Luka"));
        System.out.println(Insert_Person);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(Insert_Person).build();
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob= queryJob.waitFor();

        if(queryJob == null){
            throw new RuntimeException("Job no longer exists");
        }
        if(queryJob.getStatus().getError() != null){
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        JobStatistics.QueryStatistics statistics = queryJob.getStatistics();
        Long rowsInserted = statistics.getDmlStats().getInsertedRowCount();
        System.out.printf("%d rows inserted",rowsInserted);
    }

    static String getInsertSQL(Person... people) {
        String peopleString = Arrays.stream(people).map(person -> "(" + person.id + ",'" + person.name + "')").collect(Collectors.joining(","));
        return "INSERT INTO `bigquery-368715.db_crud.person` (id,name) VALUES " + peopleString+";";
    }

}

