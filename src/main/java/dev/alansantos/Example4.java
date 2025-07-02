package dev.alansantos;

import dev.alansantos.conf.SparkConfiguration;
import org.apache.spark.sql.SparkSession;

public class Example4 {

    /*
        Exemplo de deleção de registros no Apache Iceberg
     */
    public static void main(String[] args) {

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(SparkConfiguration.getSparkConf());

        try (final SparkSession sparkSession = sparkBuilder.getOrCreate()) {
            deleteIcebergPartitionData(sparkSession);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private static void deleteIcebergPartitionData(SparkSession sparkSession) {
        sparkSession.sql("select * from local.flights where FLIGHT_DATE = '2015-01-01'").show(false);

        sparkSession.sql("DELETE FROM local.flights WHERE FLIGHT_DATE = '2015-01-01'");

        sparkSession.sql("select * from local.flights where FLIGHT_DATE = '2015-01-01'").show(false);
    }

}