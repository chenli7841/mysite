package extractHuobiOrderbook;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertJob {

    private final BigQuery bigQuery;
    private Map<String, String> lastRecord;
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
    }

    public void insertRow(final Map<String, String> orderbookRecord) {
        if (lastRecord == null) {
            lastRecord = orderbookRecord;
            return;
        }
        final DateTime currTime = new DateTime(orderbookRecord.get("t")).withZone(DateTimeZone.UTC);
        final DateTime prevTime = new DateTime(lastRecord.get("t")).withZone(DateTimeZone.UTC);
        if (!prevTime.isBefore(currTime)) {
            return;
        } else if (prevTime.getSecondOfDay() == currTime.getSecondOfDay()) {
            lastRecord = orderbookRecord;
            return;
        } else if (prevTime.getDayOfYear() == currTime.getDayOfYear()) {
            final DateTime until = truncateToSecond(currTime);
            for (DateTime currSecond = truncateToSecond(prevTime); currSecond.isBefore(until); currSecond = addOneSecond(currSecond)) {
                insertToBigQuery(currSecond, lastRecord);
            }
            lastRecord = orderbookRecord;
        } else {
            final DateTime until = truncateToSecond(currTime);
            DateTime currSecond = truncateToSecond(prevTime);
            setupTable(currTime);
            for (; currSecond.isBefore(until); currSecond = addOneSecond(currSecond)) {
                insertToBigQuery(currSecond, lastRecord);
            }
            lastRecord = orderbookRecord;
        }
    }

    private void insertToBigQuery(final DateTime currSecond, final Map<String, String> record) {
        Map<String, String> toInsert = new HashMap<>(record);
        toInsert.put("t", addOneSecond(currSecond).toString());
        toInsert.put("localTime", DateTime.now().toString());
        final String rowId = currSecond.toString();
        InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.newBuilder(DATASET_ID, getTableName(addOneSecond(currSecond)))
                .addRow(rowId, toInsert).build());
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                System.out.println("Response error: \n" + entry.getValue());
            }
        }
    }

    private void setupTable(final DateTime dateTime) {
        templateTable.copy(DATASET_ID, getTableName(dateTime));
    }

    private String getTableName(final DateTime dateTime) {
        return String.format("%d_%d_%d", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth());
    }

    private DateTime truncateToSecond(final DateTime dateTime) {
        return dateTime.withMillisOfSecond(0);
    }

    private DateTime addOneSecond(final DateTime dateTime) {
        return dateTime.plusSeconds(1);
    }
}
