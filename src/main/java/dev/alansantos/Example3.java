package dev.alansantos;

import dev.alansantos.conf.SparkConfiguration;
import dev.alansantos.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Example3 {

    /*
        Escrita de arquivo em tabela iceberg contendo nova coluna
     */
    public static void main(String[] args) {

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(SparkConfiguration.getSparkConf());

        try (final SparkSession sparkSession = sparkBuilder.getOrCreate()) {
            writeAirlinesTableV2(sparkSession);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeAirlinesTableV2(SparkSession sparkSession) {
        Dataset<Row> airlinesDatasetV2 = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://warehouse/csv_files/airlines_v2.csv");

        sparkSession.sql("select * from local.airlines").show();

        validateNewIcebergFields(sparkSession, airlinesDatasetV2);

        airlinesDatasetV2.write()
                .format("iceberg")
                .mode("append")
                .save("local.airlines");

        sparkSession.sql("select * from local.airlines").show();
    }

    private static void validateNewIcebergFields(SparkSession sparkSession, Dataset<Row> airlinesDatasetV2) {
        StructType csvSchema = airlinesDatasetV2.schema();
        StructType tableSchema = sparkSession.table(SparkUtils.AIRLINE_TABLE).schema();
        SparkUtils.validateNewFields(sparkSession, SparkUtils.AIRLINE_TABLE, csvSchema, tableSchema);
    }


}