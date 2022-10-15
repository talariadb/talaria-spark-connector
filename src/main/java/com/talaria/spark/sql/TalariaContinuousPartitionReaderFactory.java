package com.talaria.spark.sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

/**
 * The type Talaria continuous partition reader factory.
 */
public class TalariaContinuousPartitionReaderFactory implements ContinuousPartitionReaderFactory {
    private final String tableName;
    private final StructType schema;
    private final String partitionBy;

    /**
     * Instantiates a new Talaria continuous partition reader factory.
     *
     * @param tableName   the table name
     * @param schema      the schema
     * @param partitionBy the partition by
     */
    TalariaContinuousPartitionReaderFactory(String tableName, StructType schema, String partitionBy) {
        this.tableName = tableName;
        this.schema = schema;
        this.partitionBy = partitionBy;
    }

    @Override
    public ContinuousPartitionReader<InternalRow> createReader(InputPartition partition) {
        TalariaContinuousStreamPartition p = (TalariaContinuousStreamPartition) partition;
        return new TalariaContinuousStreamPartitionReader(p.host, p.port, tableName, schema, partitionBy, p.index, p.offset);
    }
}
