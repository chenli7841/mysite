package extractHuobiOrderbook;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InsertJob {

    private final BigQuery bigQuery;
    private int currentDay;
    private int lastSecond;
    private final Table templateTable;
    private static String DATASET_ID = "huobi_orderbooks";
    private static String TEMPLATE_TABLE_ID = "template";

    public InsertJob() throws IOException {
        File credentialPath = new File("./credentials/bigquery.json");
        GoogleCredentials credentials;
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }
        bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
        templateTable = bigQuery.getTable(DATASET_ID, TEMPLATE_TABLE_ID);
        final DateTime today = DateTime.now(DateTimeZone.UTC);
        setupTable(today);
        currentDay = today.getDayOfMonth();
        lastSecond = -1;
    }

    public void insertRow(final Map<String, ?> orderbookRecord) {
        final DateTime dateTime = new DateTime(orderbookRecord.get("t")).withZone(DateTimeZone.UTC);

        if (currentDay != dateTime.getDayOfMonth()) {
            setupTable(dateTime);
        }

        final String rowId = dateTime.getSecondOfMinute() == lastSecond ?
                dateTime.toString() :
                dateTime.withMillisOfSecond(0).toString();
        InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.newBuilder(DATASET_ID, getTableName(dateTime))
                .addRow(rowId, orderbookRecord).build());
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                System.out.println("Response error: \n" + entry.getValue());
            }
        }
        lastSecond = dateTime.getSecondOfMinute();
        currentDay = dateTime.getDayOfMonth();
    }

    private void setupTable(final DateTime dateTime) {
        templateTable.copy(DATASET_ID, getTableName(dateTime));
    }

    private String getTableName(final DateTime dateTime) {
        return String.format("%d_%d_%d", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth());
    }
}
