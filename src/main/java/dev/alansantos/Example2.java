package dev.alansantos;

import dev.alansantos.conf.SparkConfiguration;
import org.apache.spark.sql.SparkSession;

public class Example2 {

    /*
        Realizando consultas em tabelas Iceberg utilizando o Spark
     */
    public static void main(String[] args) {

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(SparkConfiguration.getSparkConf());

        try (final SparkSession sparkSession = sparkBuilder.getOrCreate()) {
            numberOfFlightsByAirlineName(sparkSession);

            numberOfOriginFlightsByAirportName(sparkSession);

            numberOfDestinationFlightsByAirportName(sparkSession);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    // Realiza o COUNT de número de voos por companhia aerea
    private static void numberOfFlightsByAirlineName(SparkSession sparkSession) {
        String query = "SELECT " +
                "   COUNT(*) AS total_flights, " +
                "   a.AIRLINE " +
                "FROM local.flights f " +
                "INNER JOIN local.airlines a " +
                "   ON f.AIRLINE = a.IATA_CODE " +
                "GROUP BY a.AIRLINE";

        sparkSession.sql(query).show();
    }

    // Realiza o COUNT de aeroportos que foram origem dos voos
    private static void numberOfOriginFlightsByAirportName(SparkSession sparkSession) {
        String query = "SELECT " +
                "   COUNT(*) AS total_flights, " +
                "   a.AIRPORT as origin_airport_name " +
                "FROM local.flights f " +
                "INNER JOIN local.airports a " +
                "   ON f.ORIGIN_AIRPORT = a.IATA_CODE " +
                "GROUP BY a.AIRPORT";

        sparkSession.sql(query).show();
    }

    // Realiza o COUNT de aeroportos que foram destino dos voos
    private static void numberOfDestinationFlightsByAirportName(SparkSession sparkSession) {
        String query = "SELECT " +
                "   COUNT(*) AS total_flights, " +
                "   a.AIRPORT as origin_airport_name " +
                "FROM local.flights f " +
                "INNER JOIN local.airports a " +
                "   ON f.DESTINATION_AIRPORT = a.IATA_CODE " +
                "GROUP BY a.AIRPORT";

        sparkSession.sql(query).show();
    }
}