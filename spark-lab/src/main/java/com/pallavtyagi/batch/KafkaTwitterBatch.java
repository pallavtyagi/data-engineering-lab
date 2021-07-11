package com.pallavtyagi.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class KafkaTwitterBatch {

    public static void main(String s[]) {
        SparkSession spark = SparkSession.builder().appName("twitter-kafka-batch").master("local[*]").getOrCreate();

        Dataset<Row> df = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "twitter-events")
                .option("enable.auto.commit:", "true")
                .load();

        StructType schema = new StructType().add("text", DataTypes.StringType);

        Dataset<Row> resultSet = df
                .selectExpr("CAST(value as STRING) as tweets", "timestamp")
                .select(from_json(col("tweets"), schema).as("tweet"), col("timestamp"));

        resultSet.write().parquet("/home/pallavtyagi/Documents/workspace/data-engineering-workspace/spark-streaming/batch_data/");

    }

}
