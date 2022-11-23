package crystal.CRUD;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.IOException;

public class Delete {
    public static void main(String[] args) throws IOException, InterruptedException {
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        BigQuery bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("bigquery-368715").build().getService();

        String deleteQuery = "DELETE FROM `bigquery-368715.db_crud.person` WHERE id = 1";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(deleteQuery).build();

        Job queryJob = bigQuery.create(JobInfo.of(queryConfig));

        queryJob = queryJob.waitFor();

        if(queryJob == null){
            throw new RuntimeException("Job doesn't exist anymore");
        }
        if(queryJob.getStatus().getError() !=null){
            throw  new RuntimeException(queryJob.getStatus().getError().toString());
        }
        JobStatistics.QueryStatistics statistics = queryJob.getStatistics();
        Long deletedRowCount = statistics.getDmlStats().getDeletedRowCount();
        System.out.println("Deleted rows: " + deletedRowCount);
    }
}
