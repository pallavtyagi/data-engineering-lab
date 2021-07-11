package com.pallavtyagi.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class KafkaTwitterStream {
    public static void main(String s[]) throws TimeoutException, StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkSession spark = createLocalSparkSession("kafka-twitter-stream");
        spark.udf().register("sentiment", sentimentUdf(), DataTypes.StringType);
        Dataset<Row> df = transformation_tweets_parquet(loadKafkaStream(spark));

        df
                .writeStream()
                .format("parquet")
                .option("path", "/home/pallavtyagi/Documents/workspace/data-engineering-workspace/spark-streaming/data/")
                .option("checkpointLocation", "/home/pallavtyagi/Documents/workspace/data-engineering-workspace/spark-streaming/checkpoint/")
                .option("checkpointInterval", "1 seconds")
                .outputMode(OutputMode.Append())
                .option("truncate", false)
                .start()
                .awaitTermination();
    }


    private static Dataset<Row> transformation_sentiments(Dataset<Row> df) {
        StructType schema = new StructType().add("text", DataTypes.StringType);
        return df
                .selectExpr("CAST(value as STRING) as tweets", "timestamp")
                .select(from_json(col("tweets"), schema).as("tweet"), col("timestamp"))
                .select(callUDF("sentiment", col("tweet.text")).as("senti"), col("timestamp"))
                .withColumn("weight", lit(1L)).as("total")
                .withWatermark("timestamp", "10 minutes")
                .groupBy(col("senti"), window(col("timestamp"), "5 minutes"))
                .agg(sum(col("weight")).as("weight"));
    }

    private static Dataset<Row> transformation_tweets_parquet(Dataset<Row> df) {
        StructType schema = new StructType().add("text", DataTypes.StringType);
        return df
                .selectExpr("CAST(value as STRING) as tweets", "timestamp")
                .select(from_json(col("tweets"), schema).as("tweet"), col("timestamp"));
    }


    private static UDF1<String, String> sentimentUdf() {
        return (String text) -> {
            if (text.contains("hope") | text.contains("better") | text.contains("good"))
                return "GOOD";
            else if (text.contains("worse") | text.contains("killer") | text.contains("bad"))
                return "BAD";
            else
                return "NETURAL";
        };
    }

    private static SparkSession createLocalSparkSession(String appName) {
        return SparkSession.builder().master("local[*]").appName(appName).getOrCreate();
    }

    private static Dataset<Row> loadKafkaStream(SparkSession spark) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "twitter-events")
                .option("enable.auto.commit:", "true")
                .load();
    }

}
