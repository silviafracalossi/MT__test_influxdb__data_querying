# Test - InfluxDB - Data Querying

Tester of the InfluxDB ability of querying time series data

## Repository Structure
-   `logs/`, containing the log information of all the tests done;
-   `resources/`, containing the database credentials file and the logger properties;
-   `src/main/java`, containing the java source files;
-   `standalone/`, containing the JAR standalone version of this repository.
-   `target/`, containing the generated .class files after compiling the java code.

## Requirements
The repository is a Maven project. Therefore, the dependency that will automatically be downloaded is:
-   InfluxDB JDBC Driver (2.8)

## Installation and running the project
-   Inside the folder `resources`,
    -   Create a file called `server_influxdb_credentials.txt`, containing the username (first line) and the password (second line) to access the server InfluxDB database;
-   Run the project
    -   Open IntelliJ IDEA
    -   Compile the maven project
    -   Execute the main method in the `src/main/java/Main.java` file.

## Preparing an executable jar file
Since I couldn't manage to find a way with the command line, I used IntelliJ:
-   `File > Project Structure... `
    -   `Artifacts > + > JAR > From modules with dependencies`
        -   Select the model `test_influxdb_data_querying`
        -   Select the main class `Main.java`
        -   `Ok`
    -   Tick `Include in project build`
    -   `Ok`
-   Execute the JAR file:
    -   If you have this repository available:
        -   From the main directory, execute `java -jar standalone/DataQueryingTest.jar`.
    -   If you need a proper standalone version:
        -   Check the next paragraph.

## Preparing the standalone version on the server
-   Connect to the unibz VPN through Cisco AnyConnect;
-   Open a terminal:
    -   Execute `ssh -t sfracalossi@ironlady.inf.unibz.it "cd /data/sfracalossi ; bash"`;
    -   Execute `mkdir influxdb`;
    -   Execute `mkdir influxdb/standalone_query`;
    -   Execute `mkdir influxdb/standalone_query/resources`;
    -   Execute `mkdir influxdb/standalone_query/logs`;
-   Send the JAR and the help files from another terminal (not connected through SSH):
    -   Execute `scp standalone/DataQueryingTest.jar sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_query`;
    -   Execute `scp resources/server_influxdb_credentials.txt sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_query/resources`;
    -   Execute `scp resources/logging.properties sfracalossi@ironlady.inf.unibz.it:/data/sfracalossi/influxdb/standalone_query/resources`;
-   Execute the JAR file (use the terminal connected through SSH):
    -   Execute `cd influxdb/standalone_query`;
    -   Execute `nohup java -jar DataQueryingTest.jar [l/s] [index_name] [1GB/light] > logs/out.txt &`
