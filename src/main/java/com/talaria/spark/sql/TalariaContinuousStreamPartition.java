package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * The type Talaria continuous stream partition.
 */
public class TalariaContinuousStreamPartition implements InputPartition {
    /**
     * The Host.
     */
    String host;
    /**
     * The Port.
     */
    int port;
    /**
     * The Index.
     */
    int index;
    /**
     * The Offset.
     */
    long offset;

    /**
     * Instantiates a new Talaria continuous stream partition.
     *
     * @param host  the host
     * @param port  the port
     * @param index the index
     * @param start the start
     */
    TalariaContinuousStreamPartition(String host, int port, int index, long start) {
        this.host = host;
        this.port = port;
        this.index = index;
        this.offset = start;
    }
}
