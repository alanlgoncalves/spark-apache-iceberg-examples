package dev.alansantos;

import dev.alansantos.conf.SparkConfiguration;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

public class Example5 {

    /*
        Manutenção em todas as tabelas do catálogo
     */
    public static void main(String[] args) {

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(SparkConfiguration.getSparkConf());

        try (final SparkSession sparkSession = sparkBuilder.getOrCreate()) {

            List<String> namespaces = getNamespaces(sparkSession);

            for(String namespace : namespaces) {
                List<String> tables = getTablesFromNamespace(namespace, sparkSession);

                for(String table : tables) {
                    performTableMaintenance(sparkSession, namespace, table);
                }
            }

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private static @NotNull List<String> getTablesFromNamespace(String namespace, SparkSession sparkSession) {
        List<String> tables = sparkSession.sql(String.format("SHOW TABLES IN local.%s", namespace)).collectAsList()
                .stream().map(it -> it.getString(1)).collect(Collectors.toList());
        return tables;
    }

    private static @NotNull List<String> getNamespaces(SparkSession sparkSession) {
        List<String> namespaces = sparkSession.sql("SHOW NAMESPACES IN local").collectAsList()
                .stream().map(it -> it.getString(0)).collect(Collectors.toList());
        return namespaces;
    }

    private static void performTableMaintenance(SparkSession sparkSession, String namespace, String table) {
        sparkSession.sql(String.format("CALL local.system.expire_snapshots(" +
                "table => 'local.%s.%s'," +
                "older_than => TIMESTAMP '9999-12-31 23:59:59'," +
                "retain_last => 1)", namespace, table)).show(true);

        sparkSession.sql(String.format("CALL local.system.remove_orphan_files('local.%s.%s')", namespace, table)).show(false);

    }

}