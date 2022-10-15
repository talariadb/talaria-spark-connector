package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

/**
 * The type Talaria continuous stream partition offset.
 */
public class TalariaContinuousStreamPartitionOffset implements PartitionOffset {
    /**
     * The Offset.
     */
    long offset;

    /**
     * Instantiates a new Talaria continuous stream partition offset.
     *
     * @param offsetValue the offset value
     */
    TalariaContinuousStreamPartitionOffset(long offsetValue) {
        this.offset = offsetValue;
    }
}
