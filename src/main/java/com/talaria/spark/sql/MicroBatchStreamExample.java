package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class MicroBatchStreamExample {

    // TODO: remove examples and write integration tests.

    public static void main (String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().master("local[*]")
                //.config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                //.config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                //.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                //.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "keyfile")
                .appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.readStream()
                .format("talaria")
                .option("domain", "127.0.0.1")
                .option("port", "8043")
                .option("schema", "data")
                .option("table", "events")
                //.option("partitionFilter", "relay.outcome")
                .option("partitionFilter", "talaria-stream.click")
                .option("checkpointLocation", "file:///Users/manojbabu/talaria-spark-connector/offset")
                .load();
        df.groupBy("ingested_by").count()
           .writeStream()
           .outputMode("complete")
           .option("truncate", false)
           .format("console")
           .trigger(Trigger.ProcessingTime(10000))
           .start()
           .awaitTermination(60000*10);
    }
}