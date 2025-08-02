package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class LongTripsJob {
    public static void main(String[] args) {
        String input = null;
        String output = null;
        for (int i = 0; i < args.length; i++) {
            if ("--input".equals(args[i]) && i + 1 < args.length) {
                input = args[++i];
            } else if ("--output".equals(args[i]) && i + 1 < args.length) {
                output = args[++i];
            }
        }
        if (input == null || output == null) {
            System.err.println("Usage: LongTripsJob --input <path> --output <path>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("LongTripsJob")
                .getOrCreate();

        Dataset<Row> trips = spark.read().parquet(input);

        Dataset<Row> result = trips
                .filter(col("passenger_count").gt(2).and(col("trip_distance").gt(5)))
                .withColumn(
                        "duration_minutes",
                        expr("(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60"))
                .sort(col("duration_minutes").desc());

        result.write().mode("overwrite").parquet(output);

        spark.stop();
    }
}
