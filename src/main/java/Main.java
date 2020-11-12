import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import java.text.DecimalFormat;
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
    static QueryApi queryApi;
    static InfluxDB influxDB = null;
    static InfluxDBClient influxDBClient;

    // Databases URLs
    static final String serverURL = "http://ironmaiden.inf.unibz.it:8086";
    static final String localURL = "http://localhost:8086";
    static String requestedURL = "";

    // Databases Username, Password and Database name
    static final String username = "root";
    static final String password = "root";

    // Database Objects names
    static String dbName = "";
    static final String measurement = "temperature";
    static final String retention_policy_name = "testPolicy";
    static String bucket_name;

    // Time range
    static String time_range, now;
    final static String overall_start_time = "2000-00-00T00:00:00Z";

    // Other connection variables
    private static char[] token = "my-token".toCharArray();
    private static String org = "my-org";
    private static String bucket = "my-bucket";

    // Information about data
    static String data_loaded = "";

    // Index chosen
    static int index_no = -1;
    static String[] index_types = {"inmem", "tsi1"};

    // Logger names date formatter
    static Logger logger;
    static String logs_path = "logs/";
    static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
            "YYYY-MM-dd__HH.mm.ss");
    static DecimalFormat df = new DecimalFormat("#.00");


    public static void main(String[] args) throws IOException {

        try {

            // Getting information from user
            if (args.length != 4) {
                talkToUser();
            } else {
                requestedURL = (args[0].compareTo("l") == 0) ? localURL : serverURL;
                index_no = returnStringIndex(index_types, args[1]);
                data_loaded = args[2];
                dbName = args[3];
            }

            // Defining bucket name
            bucket_name = dbName + "/" + retention_policy_name;

            // Instantiate loggers
            logger = instantiateLogger("general");
            logger.info("Index: " + index_types[index_no]);

            // Opening a connection to the postgreSQL database
            logger.info("Connecting to the InfluxDB database...");
            createDBConnection();

            // Counting the number of rows inserted
            getDBCount();
            getNow();

            // Executing queries
            logger.info("Starting queries execution");
            allData_windowsAnalysis();
            mod_allData_windowsAnalysis();
            lastTwoDays_timedMovingAverage();
            lastThirtyMinutes_avgMaxMin();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeDBConnection();
        }
    }

    // 0. Count
    public static void getDBCount() {

        // Printing method name
        logger.info("==0. Count==");

        // Creating the query
        String count_query = "SELECT COUNT(*) FROM " + measurement;

        // Executing the query
        QueryResult queryResult = influxDB.query(new Query(count_query, dbName));
        String rows_count = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString();

        // Printing the result
        logger.info("Result: Count of rows: " + rows_count);
        System.out.println("\n0) Count of rows: " + rows_count);
    }

    // 0. Get Now - the max timestamp in the dataset
    public static void getNow() {

        // Printing method name
        logger.info("==0. GetNow==");

        // Creating and executing the query
        String count_query = "SELECT * FROM "+measurement+" ORDER BY time DESC LIMIT 1";
        QueryResult queryResult = influxDB.query(new Query(count_query, dbName));
        now = queryResult.getResults().get(0).getSeries().get(0).getValues().get(0).get(0).toString();

        // Printing the result
        time_range = "start: " + overall_start_time + ", stop: " + now;
        logger.info("Result: Max time in rows: " + now);
        System.out.println("0) Max time in rows: " + now);
    }

    //-----------------------FIRST QUERY----------------------------------------------

    // For windows of 30 minutes, calculate mean, max and min.
    public static void allData_windowsAnalysis() {

        // Printing method name
        System.out.println("\n1a) allData_windowsAnalysis");

        // Creating the query
        String window_size = "30m";
        String allData_query = "mean = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> mean()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\"])\n" +

                "max = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> max()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\",\"_time\"])\n" +

                "min = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> min()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\",\"_time\"])\n" +

                "first_join = join(tables: {mean:mean, max:max}, on: [\"_start\", \"_stop\"])\n" +

                "join(tables: {first_join:first_join, min:min}, on: [\"_start\", \"_stop\"])\n" +
                " |> yield()\n";

        // Executing the query
        logger.info("Executing windowsAnalysis on AllData");
        List<FluxTable> tables = queryApi.query(allData_query);
        logger.info("Completed execution");

        // Printing the result
        printFirstQuery(tables);
    }

    // Printing the results from the first query
    public static void printFirstQuery(List<FluxTable> tables) {

        // Iterating through tables (in this case: only "temperature" table)
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();

            // Iterating through all the rows
            for (FluxRecord fluxRecord : records) {
                logger.info("Result:" +
                        " From " + fluxRecord.getValueByKey("_start") +
                        " to " + fluxRecord.getValueByKey("_stop") +
                        " AVG: " + df.format(fluxRecord.getValueByKey("_value_mean")) +
                        " Max: " + fluxRecord.getValueByKey("_value_max") +
                        " Min: " + fluxRecord.getValueByKey("_value"));
            }
        }
    }

    //-----------------------MOD FIRST QUERY----------------------------------------------

    // For windows of 30 minutes, calculate mean, max and min.
    public static void mod_allData_windowsAnalysis() {

        // Printing method name
        System.out.println("1b) mod_allData_windowsAnalysis");

        // Creating the query
        String window_size = "30m";
        String allData_query = "mean = from(bucket: \"" + bucket_name + "\")\n" +
                "  |> range(" + time_range + ")\n" +
                "  |> filter(fn: (r) =>  r._measurement == \"" + measurement + "\" and (r._field == \"value\"))\n" +
                "  |> window(every: " + window_size + ") \n" +
                "  |> mean()\n" +
                "  |> drop(columns: [\"_field\", \"_measurement\", \"table\"])\n" +
                "  |> yield() \n";

        // Executing the query
        logger.info("Executing mod_windowsAnalysis on AllData");
        List<FluxTable> tables = queryApi.query(allData_query);
        logger.info("Completed execution");

        // Printing the result
        mod_printFirstQuery(tables);
    }

    // Printing the results from the first query
    public static void mod_printFirstQuery(List<FluxTable> tables) {

        // Iterating through tables (in this case: only "temperature" table)
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();

            // Iterating through all the rows
            for (FluxRecord fluxRecord : records) {
                logger.info("Result:" +
                        " From " + fluxRecord.getValueByKey("_start") +
                        " to " + fluxRecord.getValueByKey("_stop") +
                        " AVG: " + fluxRecord.getValueByKey("_value") +
                        " Max: " + fluxRecord.getValueByKey("_value") +
                        " Min: " + fluxRecord.getValueByKey("_value"));
            }
        }
    }

    //-----------------------SECOND QUERY----------------------------------------------
    // Every 2 minutes of data, computes the average of the current temperature value
    //      and the ones of the previous 4 minutes on last 2 days of data
    public static void lastTwoDays_timedMovingAverage() {

        // Printing method name
        System.out.println("2) lastTwoDays_timedMovingAverage");

        // Creating the query
        String lastTwoDays_query = "import \"experimental\"\n" +
                "from(bucket:\"" + bucket_name + "\")" +
                " |> range(start: experimental.subDuration( \n" +
                "                  d: 2d, \n" +
                "                  from: "+now+")) \n" +
                " |> filter(fn:(r) => " +
                "       r._measurement == \"" + measurement + "\"" +
                " )" +
                " |> timedMovingAverage(every: 2m, period: 4m)";

        // Executing the query
        logger.info("Executing timedMovingAverage on LastTwoDays");
        List<FluxTable> tables = queryApi.query(lastTwoDays_query);
        logger.info("Completed execution");

        // Printing the result
        printSecondQuery(tables);
    }

    // Printing the results from the second query
    public static void printSecondQuery(List<FluxTable> tables) {

        // Iterating through tables (in this case: only "temperature" table)
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();

            // Iterating through all the rows
            for (FluxRecord fluxRecord : records) {
                double value = Double.parseDouble(fluxRecord.getValueByKey("_value") + "");
                logger.info("Result: " + fluxRecord.getValueByKey("_time") + " " + df.format(value));
            }
        }
    }

    //-----------------------THIRD QUERY----------------------------------------------
    // 3. Calculate mean, max and min on last (arbitrary) 30 minutes of data
    public static void lastThirtyMinutes_avgMaxMin() {

        // Printing method name
        System.out.println("3) lastThirtyMinutes_avgMaxMin");

        // Creating the query
        String lastThirtyMinutes_query = "" +
                " SELECT MEAN(value), MAX(value), MIN(value) " +
                " FROM " + measurement +
                " WHERE time >= '" + now + "' - 30m ";

        // Executing the query
        logger.info("Executing AvgMaxMin on LastThirtyMinutes");
        QueryResult queryResult = influxDB.query(new Query(lastThirtyMinutes_query, dbName));
        logger.info("Completed execution");

        // Printing the result
        printThirdQuery(queryResult, now);
    }

    // Printing the results from the third query
    public static void printThirdQuery(QueryResult qr, String now) {

        // Getting all the variables
        List<List<Object>> values = qr.getResults().get(0).getSeries().get(0).getValues();

        // Printing the result
        logger.info("Result:" +
                " From " + values.get(0).get(0) +
                " to " + now +
                " AVG: " + df.format(values.get(0).get(1)) +
                " Max: " + values.get(0).get(2) +
                " Min: " + values.get(0).get(3));
    }

    //-----------------------UTILITY----------------------------------------------

    // Understanding whether the user wants the sever db or the local db
    public static void talkToUser() {

        // Instantiating the input scanner
        Scanner sc = new Scanner(System.in);
        String response = "";

        // While the answer is not correct
        while (requestedURL.compareTo("") == 0) {
            System.out.print("Where do you want the script to be executed?"
                    + " (\"s\" for server, \"l\" for local): ");
            response = sc.nextLine().replace(" ", "");

            // Understanding what the user wants
            if (response.compareTo("l") == 0) {
                requestedURL = localURL;
            }
            if (response.compareTo("s") == 0) {
                requestedURL = serverURL;
            }
        }

        // Understanding what the index configured
        while (index_no == -1) {
            System.out.print("What is the index configured right now?"
                    + " (Type \"inmem\" or \"tsi1\"): ");
            response = sc.nextLine().replace(" ", "");
            index_no = returnStringIndex(index_types, response);
        }

        // Understanding the DB table
        while (dbName.compareTo("test_table") != 0 && dbName.compareTo("test_table_n") != 0) {
            System.out.print("What is the name of the database?"
                    + " (Type \"test_table\" or \"test_table_n\"): ");
            dbName = sc.nextLine().replace(" ", "");
        }

        // Understanding what the index configured
        while (data_loaded.compareTo("1GB") != 0 && data_loaded.compareTo("light") != 0) {
            System.out.print("What data is uploaded?"
                    + " (Type \"1GB\" or \"light\"): ");
            data_loaded = sc.nextLine().replace(" ", "");
        }
    }

    // Instantiating the logger for the general information or errors
    public static Logger instantiateLogger(String file_name) throws IOException {

        // Retrieving and formatting current timestamp
        Date date = new Date();
        Timestamp now = new Timestamp(date.getTime());
        String dateAsString = simpleDateFormat.format(now);

        // Setting the name of the folder
        if (file_name.compareTo("general") == 0) {
            logs_path += dateAsString + "/";
            File file = new File(logs_path);
            file.mkdirs();
        }

        // Instantiating general logger
        String log_complete_path = logs_path + dateAsString + "__influxdb_data_querying.xml";
        Logger logger = Logger.getLogger("DataQueryingGeneralLog_" + file_name);
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
        for (int i = 0; i < list.length; i++) {
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
            logger.severe("Failed connecting to the Database InfluxDB");
        } else {

            // Setting a larger read timeout
            OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();
            okHttpClientBuilder.readTimeout(1, TimeUnit.HOURS);
            InfluxDBClientOptions options = new InfluxDBClientOptions.Builder()
                .url(requestedURL)
                .authenticateToken(token)
                .org(org)
                .bucket(bucket)
                .okHttpClient(okHttpClientBuilder)
                .build();

            // Connecting the influxdb client for flux too
            influxDBClient = InfluxDBClientFactory.create(options);
            queryApi = influxDBClient.getQueryApi();
        }
    }

    // Closing the connections to the database
    public static void closeDBConnection() {
        try {
            influxDB.close();
            influxDBClient.close();
        } catch (NullPointerException e) {
            logger.severe("Closing DB connection - NullPointerException");
        }
    }
}