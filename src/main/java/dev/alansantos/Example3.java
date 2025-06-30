package dev.alansantos;

import dev.alansantos.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Example3 {

    /*
        Escrita de arquivo em tabela iceberg contendo nova coluna
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