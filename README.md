<div align="center">
    <img width="300" height="225" src="assets/talaria-spark-logo.png">
</div>

# Apache Spark Connector for TalariaDB

[Talaria](https://www.github.com/talariadb/talaria) is a distributed time 
series database which stores immutable data originating from various event
sourcing applications.

That said, it's primarily an Event Store backed by [BadgerDB](https://github.com/dgraph-io/badger) a performant alternative to non-Go-based key-value stores like RocksDB.

Talaria listens to the event streams using: gRPC, AWS SQS Polling & NATS subject.

Talaria also, fans out the received event records to various cloud data-storages like:
 - AWS S3
 - Google Cloud Storage
 - Azure Storage
 - Google Bigquery
 - Google Pubsub
 - Or another Talaria cluster which can serve like a read replica.

The events are exposed to other analytic query engines like <b>trino/presto/spark</b> via thrift RPC or gRPC.

This repo utilizes gRPC endpoints to query the data in talaria from apache spark application via:
 - Batch Processing
 - MicroBatch Processing &
 - Continuous Stream Processing (Experimental Spark API)

## Quick Start

`example.java`

```
// Example Java Application

package com.talaria.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

public class MicroBatchStreamExample {

    public static void main (String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().master("local[*]")
                .appName("Simple Application").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.readStream()
                .format("talaria")
                .option("domain", "<talaria-domain>")
                .option("port", "<talaria-grpc-port>")
                .option("schema", "<schema-name>")
                .option("table", "<table-name>")
                .option("partitionFilter", "hashby-column-value")
                .option("checkpointLocation", "file:///<path-to-checkpoint-file>")
                .load();
        df.groupBy("<sorty-by-column-name>").count()
           .writeStream()
           .outputMode("complete")
           .option("truncate", false)
           .format("console")
           .trigger(Trigger.ProcessingTime(10000))
           .start()
           .awaitTermination(60000*10);
    }
}
```

`spark-defaults.conf`

```
spark.driver.extraClassPath /opt/spark/talaria-spark-connector-all-1.0-SNAPSHOT.jar
spark.executor.extraClassPath /opt/spark/talaria-spark-connector-all-1.0-SNAPSHOT.jar
```

or specify the jar using `--jars <path-to-talaria-spark-connector.jar>`.


## Implementation Details

Please refer to the [docs](./docs/overview.md).