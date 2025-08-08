package org.bigdata.taxi;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class TaxiAnalytics {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: TaxiAnalytics <tripsParquetPath> <zonesCsvPath> <outputBase>");
            System.exit(1);
        }
        String tripsPath = args[0];
        String zonesPath = args[1];
        String outBase = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("Taxi Analytics - Java")
                .getOrCreate();

        // Read inputs
        Dataset<Row> trips = spark.read().parquet(tripsPath);
        Dataset<Row> zones = spark.read().option("header", "true").csv(zonesPath);

        // Ensure useful columns have expected types
        // timestamps are usually already timestamp in Parquet; but cast just in case
        trips = trips
                .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
                .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
                .withColumn("passenger_count", col("passenger_count").cast(DataTypes.IntegerType))
                .withColumn("trip_distance", col("trip_distance").cast(DataTypes.DoubleType))
                .withColumn("fare_amount", col("fare_amount").cast(DataTypes.DoubleType))
                .withColumn("total_amount", col("total_amount").cast(DataTypes.DoubleType))
                .withColumn("tip_amount", col("tip_amount").cast(DataTypes.DoubleType))
                .withColumn("PULocationID", col("PULocationID").cast(DataTypes.IntegerType))
                .withColumn("DOLocationID", col("DOLocationID").cast(DataTypes.IntegerType));

        zones = zones
                .withColumn("LocationID", col("LocationID").cast(DataTypes.IntegerType));

        // Q1: trips with passenger_count > 2 and trip_distance > 5
        // Add duration_minutes and order desc
        Column durationMinutes = expr("(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0");
        Dataset<Row> q1 = trips
                .filter(col("passenger_count").gt(2).and(col("trip_distance").gt(5)))
                .withColumn("duration_minutes", durationMinutes)
                .orderBy(col("duration_minutes").desc());
        q1.write().mode(SaveMode.Overwrite).parquet(outBase + "/q1_long_trips.parquet");

        // Q2: average fare_amount by Zone for pickup locations
        Dataset<Row> tripsPU = trips.join(zones, trips.col("PULocationID").equalTo(zones.col("LocationID")), "left");
        Dataset<Row> q2 = tripsPU.groupBy(col("Zone"), col("Borough"))
                .agg(avg(col("fare_amount")).alias("avg_fare_amount"))
                .orderBy(col("avg_fare_amount").desc());
        q2.write().mode(SaveMode.Overwrite).parquet(outBase + "/q2_avg_fare_by_zone.parquet");

        // Q3: total_amount sum and trip count by Borough for dropoff
        Dataset<Row> tripsDO = trips.join(zones, trips.col("DOLocationID").equalTo(zones.col("LocationID")), "left");
        Dataset<Row> q3 = tripsDO.groupBy(col("Borough"))
                .agg(sum(col("total_amount")).alias("sum_total_amount"),
                     count(lit(1)).alias("trip_count"))
                .orderBy(col("sum_total_amount").desc());
        q3.write().mode(SaveMode.Overwrite).parquet(outBase + "/q3_revenue_by_borough.parquet");

        // Q4: per day, maximum tip_amount
        Dataset<Row> q4 = trips
                .withColumn("date", to_date(col("tpep_pickup_datetime")))
                .groupBy(col("date"))
                .agg(max(col("tip_amount")).alias("max_tip_amount"))
                .orderBy(col("date").asc());
        q4.write().mode(SaveMode.Overwrite).parquet(outBase + "/q4_max_tip_per_day.parquet");

        spark.stop();
    }
}
