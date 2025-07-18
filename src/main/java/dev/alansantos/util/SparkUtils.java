package dev.alansantos.util;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class SparkUtils {

    public final static String AIRPORT_TABLE = "local.hub.airports";

    public final static String AIRLINE_TABLE = "local.hub.airlines";

    public final static String FLIGHT_TABLE = "local.hub.flights";

    public static void validateNewFields(final SparkSession sparkSession,
                                          final String icebergTableName,
                                          final StructType fileSchema,
                                          final StructType icebergSchema) {

        for (StructField field : fileSchema.fields()) {
            if (!Arrays.asList(icebergSchema.fields()).contains(field)) {
                String colName = field.name();
                String type = field.dataType().catalogString();

                sparkSession.sql(
                        String.format("ALTER TABLE %s ADD COLUMN %s %s", icebergTableName, colName, type)
                );
            }
        }
    }
}
