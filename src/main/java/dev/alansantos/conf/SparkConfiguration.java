package dev.alansantos.conf;

import org.apache.spark.SparkConf;

public class SparkConfiguration {

    public static SparkConf getSparkConf() {
        return new SparkConf()
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
    }

}
