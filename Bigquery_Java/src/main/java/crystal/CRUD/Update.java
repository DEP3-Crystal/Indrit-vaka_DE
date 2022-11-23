package crystal.CRUD;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.IOException;

public class Update {
    public static void main(String[] args) throws IOException, InterruptedException {
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId("bigquery-368715").setCredentials(credentials).build().getService();

        final String update = "UPDATE `bigquery-368715.db_crud.person` SET id = 3 WHERE name='Luka' AND id = 2;";

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(update).build();

        Job jobQuery = bigQuery.create(JobInfo.newBuilder(queryConfig).build());
        jobQuery = jobQuery.waitFor();
        if(jobQuery == null){
            throw new RuntimeException("job no longer exists");
        }
        else if (jobQuery.getStatus().getError() != null) {
            throw new RuntimeException(jobQuery.getStatus().getError().toString());
        }
        JobStatistics.QueryStatistics statistics = jobQuery.getStatistics();
        Long updatedRowCount = statistics.getDmlStats().getUpdatedRowCount();
        System.out.println(updatedRowCount);

    }
}
