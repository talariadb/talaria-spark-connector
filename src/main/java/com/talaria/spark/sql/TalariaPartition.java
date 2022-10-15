package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;

/*
   TalariaPartition: implements InputPartition to specify the partition details
         for executors to extract data. InputPartition extends Serializable class.
         so one should pass primitive types(or anything which extends java serializable)
         as the partition class object is serialized into JVM Bytecode and sent to executors
         within Task objects. The executor will then process the query given the partition details.
 */
public class TalariaPartition implements InputPartition{
    public final String host;
    public final int port;
    public final Long start;
    public final Long end;
    TalariaPartition(String host, int port, Long start, Long end) {
       this.host = host;
       this.port = port;
       this.start = start;
       this.end = end;
    }
}