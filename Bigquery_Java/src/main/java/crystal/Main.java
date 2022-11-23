package crystal;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;
import com.google.cloud.bigquery.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        //GOOGLE_APPLICATION_CREDENTIALS=~/Downloads/your-service-account-file.json mvn exec:java -Dexec.mainClass="com.sohamkamani.Select" -Dexec.classpathScope=runtime
        BigQuery bigQuery = BigQueryOptions.newBuilder()
                .setProjectId("bigquery-368715")
                .build()
                .getService();
        final String GET_WORD_COUNT = "SELECT word, word _ count FROM `bigquery-public-data.samples.shakespeare` where ORDER BY word _ count DESC limit 10;";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(GET_WORD_COUNT).build();

        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();
        if (queryJob == null) {
            throw new RuntimeException("job not longer exists");
        }
        if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        System.out.println("word\tword_count");
        TableResult results = queryJob.getQueryResults();
        for (var row : results.iterateAll()) {
            String word = row.get("word").getStringValue();
            int wordCount = row.get("word_count").getNumericValue().intValue();
            System.out.printf("%s\t%d\n", word, wordCount);
        }


    }
}