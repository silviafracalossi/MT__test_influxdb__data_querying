import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

public class Main {

    // DB variables
    static InfluxDB influxDB = null;
    static InfluxDBClient influxDBClient;

    // Databases URLs
    static final String serverURL = "http://ironmaiden.inf.unibz.it:8086";
    static final String localURL = "http://localhost:8086";
    static String requestedURL = serverURL;

    // Databases Username, Password and Database name
    static final String username = "root";
    static final String password = "root";

    // Database Objects names
    static final String dbName = "test_table";
    static final String measurement = "temperature";
    static final String retention_policy_name = "testPolicy";
    static final String bucket_name = dbName+"/"+retention_policy_name;

    // Other connection variables
    private static char[] token = "my-token".toCharArray();
    private static String org = "my-org";
    private static String bucket = "my-bucket";

    // Query callers
    static QueryResult queryResult;
    static QueryApi queryApi;

    // Variables of queries
    static String window_aggr_duration = "1h";
    static String count_query = "SELECT COUNT(*) FROM " +measurement;
    static String flux_query = "";

    // Logger names date formatter
    static Logger general_logger;
    static String logs_path = "logs/";
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
            "YYYY-MM-dd__HH.mm.ss");


    public static void main(String[] args) throws IOException {

        // Instantiating the input scanner
        Scanner sc = new Scanner(System.in);
        String response = "";

        try {

            // Instantiate general logger
            general_logger = instantiateLogger("general");

            // TODO decomment
//            // Understanding whether the user wants the sever db or the local db
//            boolean correct_answer = false;
//            while (!correct_answer) {
//                System.out.print("Where do you want the script to be executed?"
//                        +" (\"s\" for server, \"l\" for local): ");
//                response = sc.nextLine().replace(" ", "");
//
//                // Understanding what the user wants
//                if (response.compareTo("l") == 0 || response.compareTo("s") == 0) {
//                    correct_answer=true;
//                    if (response.compareTo("l") == 0) {
//                        requestedURL = localURL;
//                    }
//                }
//            }
            requestedURL = localURL;

            // Opening a connection to the postgreSQL database
            general_logger.info("Connecting to the InfluxDB database...");
            createDBConnection();

//            // Asking if the user is ready  TODO decomment
//            System.out.print("\t\t==\"Ready Statement\"==\n" +
//                    "Confirm the other test script, "+
//                    "then come back here and press enter. ");
//            response = sc.nextLine();

            // Marking start of tests
            general_logger.info("Starting queries execution");

            // Execute queries forever
            doQueries();

            // Execute query for data analysis tests
            doWindowAggregate();
            doHistoryVisualization();
            doAnalysisQuery();

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            closeDBConnection();
        }
    }

    // Method that computes concurrent data analysis query requests
    public static void doQueries () {

        while (true) {
            try {

                // Count Query
                printDBCount();

                // Analysis Query
                doAnalysisQuery();

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Exception. Starts over.");
            }
        }
    }

    // 0. Count
    public static void printDBCount() {
        queryResult = influxDB.query(new Query(count_query, dbName));
        System.out.println("\n-- Count: "+ queryResult.getResults().get(0).getSeries()
                .get(0).getValues().get(0).get(1));
    }

    // 1. Window Aggregate
    public static void doWindowAggregate() {
        System.out.println("-- 1.Window Aggregate: "+window_aggr_duration);
        List<FluxTable> tables = queryApi.query(
            "from(bucket:\""+bucket_name+"\")" +
                " |> range(start: 2017-12-01T00:00:00Z, stop: 2017-12-05T00:00:00Z)" +
                " |> filter(fn:(r) => " +
                "r._measurement == \""+measurement+"\"" +
                ")" +
                " |> aggregateWindow(every: "+window_aggr_duration+", fn: mean)"
        );

        // Print rows and rows count
        //printResultRows(tables);
        //printResultRowsCount(tables);
    }

    // 2. History Visualization
    public static void doHistoryVisualization() {
        System.out.println("-- 2.History Visualization");
        List<FluxTable> tables = queryApi.query(
        "from(bucket:\""+bucket_name+"\")" +
                " |> range(start: 2017-01-01T00:00:00Z, stop: 2017-12-05T00:00:00Z)"
        );

        // Print rows and rows count
        //printResultRows(tables);
        //printResultRowsCount(tables);
    }

    // 3. Analysis Query
    public static void doAnalysisQuery() {
        System.out.println("-- 3.Analysis Query");
        List<FluxTable> tables = queryApi.query(
                "from(bucket:\""+bucket_name+"\")" +
                        " |> range(start: 2017-12-01T00:00:00Z, stop: 2017-12-05T00:00:00Z)" +
                        " |> filter(fn:(r) => " +
                        "r._measurement == \""+measurement+"\"" +
                        ")" +
                        " |> movingAverage(n: 2000)"
        );

        // Print rows and rows count
        //printResultRows(tables);
        //printResultRowsCount(tables);
    }


    //-----------------------ROWS PRINTS----------------------------------------------

    // Goes through result rows and counts them
    public static void printResultRowsCount(List<FluxTable> tables) {
        int count = 0;

        // Iterating through tables (in this case: only "temperature" table)
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();

            // Iterating through all the rows
            for (FluxRecord fluxRecord : records) {
                count++;
            }
        }

        System.out.println("Rows are "+count);
    }

    // Prints result rows
    public static void printResultRows(List<FluxTable> tables) {
        // Iterating only on "temperature"
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();

            // Iterating through all the rows
            for (FluxRecord fluxRecord : records) {
                System.out.println(fluxRecord.getTime() + ": " + fluxRecord.getValueByKey("_value"));
            }
        }
    }


    //-----------------------UTILITY----------------------------------------------

    // Instantiating the logger for the general information or errors
    public static Logger instantiateLogger (String file_name) throws IOException {

        // Retrieving and formatting current timestamp
        Date date = new Date();
        Timestamp now = new Timestamp(date.getTime());
        String dateAsString = simpleDateFormat.format(now);

        // Setting the name of the folder
        if (file_name.compareTo("general") == 0) {
            logs_path += dateAsString+"/";
            File file = new File(logs_path);
            boolean bool = file.mkdirs();
        }

        // Instantiating general logger
        String log_complete_path = logs_path + dateAsString + "__influxdb_data_querying.xml";
        Logger logger = Logger.getLogger("DataQueryingGeneralLog_"+file_name);
        logger.setLevel(Level.ALL);

        // Loading properties of log file
        Properties preferences = new Properties();
        try {
            FileInputStream configFile = new FileInputStream("resources/logging.properties");
            preferences.load(configFile);
            LogManager.getLogManager().readConfiguration(configFile);
        } catch (IOException ex) {
            System.out.println("[WARN] Could not load configuration file");
        }

        // Instantiating file handler
        FileHandler gl_fh = new FileHandler(log_complete_path);
        logger.addHandler(gl_fh);

        // Returning the logger
        return logger;
    }


    // Returns the index_no of the specified string in the string array
    public static int returnStringIndex(String[] list, String keyword) {
        for (int i=0; i<list.length; i++) {
            if (list[i].compareTo(keyword) == 0) {
                return i;
            }
        }
        return -1;
    }

    //----------------------DATABASE----------------------------------------------

    // Connecting to the InfluxDB database
    public static void createDBConnection() {

        // Connecting to the DB
        influxDB = InfluxDBFactory.connect(requestedURL, username, password);

        // Pinging the DB
        Pong response = influxDB.ping();

        // Printing a message in case of failed connection
        if (response.getVersion().equalsIgnoreCase("unknown")) {
            System.out.println("Failed connecting to the Database InfluxDB");
        } else {

            // Connecting the influxdb client for flux too
            influxDBClient = InfluxDBClientFactory.create(localURL, token, org, bucket);
            queryApi = influxDBClient.getQueryApi();
        }
    }

    // Closing the connections to the database
    public static void closeDBConnection() {
        try {
            influxDB.close();
        } catch (NullPointerException e) {
            System.out.println("Closing DB connection - NullPointerException");
        }
    }
}
