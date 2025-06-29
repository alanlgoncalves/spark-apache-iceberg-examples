package dev.alansantos;

import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("spark-apache-iceberg-examples")
                .master("local[*]");

        try (final SparkSession sparkSession = sparkBuilder.getOrCreate()) {



        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}