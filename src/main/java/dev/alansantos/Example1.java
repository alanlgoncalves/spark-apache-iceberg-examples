package dev.alansantos;

import dev.alansantos.conf.SparkConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static dev.alansantos.util.SparkUtils.*;

public class Example1 {

    /*
        Escrita dos 3 CSV (airlines.csv, airports.csv, e flights.csv) em tabelas Iceberg
     */
    public static void main(String[] args) {

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(SparkConfiguration.getSparkConf());

        try (final SparkSession sparkSession = sparkBuilder.getOrCreate()) {
            createFlightsTable(sparkSession);

            createAirlinesTable(sparkSession);

            createAirportsTable(sparkSession);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private static void createFlightsTable(SparkSession sparkSession) {
        Dataset<Row> flightsDataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://warehouse/csv_files/flights.csv");

        flightsDataset.createOrReplaceTempView("tmp_flights_view");

        if(!sparkSession.catalog().tableExists(FLIGHT_TABLE)) {
            sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS local.hub");

            sparkSession.sql(
                    "CREATE TABLE IF NOT EXISTS local.hub.flights \n" +
                    "    USING iceberg \n" +
                    "    PARTITIONED BY (days(FLIGHT_DATE)) \n" +
                    "    TBLPROPERTIES (\n" +
                    "        'write.target-file-size-bytes' = '134217728', \n" +
                    "        'write.metadata.previous-versions-max' = '5', \n" +
                    "        'write.metadata.delete-after-commit.enabled' = 'true' \n" +
                    "   )" +
                    "   AS \n" +
                    "   SELECT * FROM tmp_flights_view");
        }

        flightsDataset.write()
                .format("iceberg")
                .mode("append")
                .save("local.hub.flights");
    }

    private static void createAirportsTable(SparkSession sparkSession) {
        Dataset<Row> airlinesDataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://warehouse/csv_files/airports.csv");

        airlinesDataset.createOrReplaceTempView("tmp_airports_view");

        if(!sparkSession.catalog().tableExists(AIRPORT_TABLE)) {
            sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS local.hub");

            sparkSession.sql("CREATE TABLE IF NOT EXISTS local.hub.airports \n" +
                    "    USING iceberg \n" +
                    "    TBLPROPERTIES (\n" +
                    "        'write.target-file-size-bytes' = '134217728', \n" +
                    "        'write.metadata.previous-versions-max' = '5', \n" +
                    "        'write.metadata.delete-after-commit.enabled' = 'true' \n" +
                    "   )" +
                    "   AS \n" +
                    "   SELECT * FROM tmp_airports_view");
        }

        airlinesDataset.write()
                .format("iceberg")
                .mode("append")
                .save("local.hub.airports");
    }

    private static void createAirlinesTable(SparkSession sparkSession) {
        Dataset<Row> airlinesDataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://warehouse/csv_files/airlines.csv");

        airlinesDataset.createOrReplaceTempView("tmp_airlines_view");

        if(!sparkSession.catalog().tableExists(AIRLINE_TABLE)) {
            sparkSession.sql("CREATE NAMESPACE IF NOT EXISTS local.hub");

            sparkSession.sql("CREATE TABLE IF NOT EXISTS local.hub.airlines \n" +
                    "    USING iceberg \n" +
                    "    TBLPROPERTIES (\n" +
                    "        'write.target-file-size-bytes' = '134217728', \n" +
                    "        'write.metadata.previous-versions-max' = '5', \n" +
                    "        'write.metadata.delete-after-commit.enabled' = 'true' \n" +
                    "   )" +
                    "   AS \n" +
                    "   SELECT * FROM tmp_airlines_view");
        }

        airlinesDataset.write()
                .format("iceberg")
                .mode("append")
                .save("local.hub.airlines");
    }
}