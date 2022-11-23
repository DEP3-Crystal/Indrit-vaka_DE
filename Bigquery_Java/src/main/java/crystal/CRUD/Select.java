package crystal.CRUD;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.UUID;

public class Select {
    public static void main(String[] args) throws InterruptedException, IOException {
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault()
                .createScoped(
                        ImmutableSet.of("https://www.googleapis.com/auth/bigquery",
                                "https://www.googleapis.com/auth/drive"));
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId("bigquery-368715")
                .build()
                .getService();


        final String queryString = "SELECT * FROM `bigquery-368715.db_crud.person`";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        TableResult queryResults = queryJob.getQueryResults();
        for (var row: queryResults.iterateAll()) {
            int id = Integer.parseInt(row.get("id").getStringValue());
            var name =row.get("name").getStringValue();
            System.out.printf("id: %d, name: %s\n",id,name);
        }

    }
}
