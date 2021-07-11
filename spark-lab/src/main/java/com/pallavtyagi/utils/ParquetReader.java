package com.pallavtyagi.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;

public class ParquetReader {
    public static void main(String s[]) {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("ParquetReader")
                .master("local[*]")
                .getOrCreate();


        long num = spark.read().parquet("/home/pallavtyagi/Documents/workspace/data-engineering-workspace/spark-streaming/data/")
                .select("tweet.text", "timestamp")
                .select(regexp_replace(col("text"), "\n", " ").as("tweet"))
                .count();
//                .show(100000, false);
        System.out.println(num);


    }

}
