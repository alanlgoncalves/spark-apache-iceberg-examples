package dev.alansantos;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example1 {

    /*
        Escrita dos 3 CSV (airlines.csv, airports.csv, e flights.csv) em tabelas Iceberg
     */
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
            .setAppName("IcebergCSVToS3")
            .setMaster("local[*]")
            .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .set("spark.hadoop.fs.s3a.path.style.access", "true")
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.local.type", "hadoop")
            .set("spark.sql.catalog.local.warehouse", "s3a://warehouse/database");

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(sparkConf);

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

        if(!sparkSession.catalog().tableExists("flights")) {
            sparkSession.sql(
                    "CREATE TABLE IF NOT EXISTS local.flights \n" +
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
                .save("local.flights");
    }

    private static void createAirportsTable(SparkSession sparkSession) {
        Dataset<Row> airlinesDataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://warehouse/csv_files/airports.csv");

        airlinesDataset.createOrReplaceTempView("tmp_airports_view");

        if(!sparkSession.catalog().tableExists("airports")) {
            sparkSession.sql("CREATE TABLE IF NOT EXISTS local.airports \n" +
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
                .save("local.airports");
    }

    private static void createAirlinesTable(SparkSession sparkSession) {
        Dataset<Row> airlinesDataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://warehouse/csv_files/airlines.csv");

        airlinesDataset.createOrReplaceTempView("tmp_airlines_view");

        if(!sparkSession.catalog().tableExists("airlines")) {
            sparkSession.sql("CREATE TABLE IF NOT EXISTS local.airlines \n" +
                    "    USING iceberg \n" +
                    "    TBLPROPERTIES (\n" +
                    "        'write.target-file-size-bytes' = '134217728', \n" +
                    "        'write.metadata.previous-versions-max' = '5', \n" +
                    "        'write.metadata.delete-after-commit.enabled' = 'true' \n" +
                    "   )" +
                    "   AS \n" +
                    "   SELECT * FROM tmp_airlines_view");
        }
    }
}