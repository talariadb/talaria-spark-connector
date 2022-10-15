package com.talaria.spark.sql;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/*
    TalariaPartitionReaderFactory: implements PartitionReaderFactory the methods
     to create a PartitionReader using factory pattern.
 */
public class TalariaPartitionReaderFactory implements PartitionReaderFactory {

    private final String tableName;
    private final StructType schema;
    private final String partitionFilter;
    private final String hashBy;
    private final String sortBy;
    private final String talariaSchema;


    TalariaPartitionReaderFactory(String tableName, StructType schema, String tschema, String hashBy, String sortBy, String partitionFilter){
        this.tableName = tableName;
        this.schema = schema;
        this.partitionFilter = partitionFilter;
        this.talariaSchema = tschema;
        this.hashBy = hashBy;
        this.sortBy = sortBy;
    }

    /*
        PartitionReader<InternalRow> createReader: overridden method to specify
        querying row-oriented partitions.
     */
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return null;
    }

    /*
        PartitionReader<ColumnarBatch> createColumnarReader: overridden method to
        specify implementation details for querying talaria in columnar way.
     */

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
        Preconditions.checkArgument(partition instanceof TalariaPartition, "partition provided is not TalariaPartition");
        TalariaPartition p = (TalariaPartition) partition;
        return new TalariaColumnarPartitionReader(p.host, p.port, tableName, schema, talariaSchema, hashBy, sortBy, partitionFilter, p.start, p.end);
    }

    /*
        supportColumnarReads: specifies spark query planner to use columnar reader
        instead of internal row version.
     */

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
        return true;
    }
}