package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import com.talaria.client.TalariaClient;
import com.talaria.protos.GetRowsResponse;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * The type Talaria continuous stream partition reader.
 */
public class TalariaContinuousStreamPartitionReader implements ContinuousPartitionReader<InternalRow> {
    /**
     * The Tc.
     */
    TalariaClient tc;
    /**
     * The Split id.
     */
    ByteString splitID;
    /**
     * The Columns.
     */
    List<String> columns;
    /**
     * The Rows data.
     */
    GetRowsResponse rowsData;
    /**
     * The Table name.
     */
    String tableName;
    /**
     * The Partition by.
     */
    String partitionBy;
    /**
     * The It.
     */
    Iterator<InternalRow> it;
    /**
     * The Row.
     */
    InternalRow row;

    private final long offset;
    private final long increment = 1000L;
    private long nextOffset;
    /**
     * The Util.
     */
    SparkUtil util = new SparkUtil();

    /**
     * Instantiates a new Talaria continuous stream partition reader.
     *
     * @param host           the host
     * @param port           the port
     * @param tableName      the table name
     * @param schema         the schema
     * @param partitionBy    the partition by
     * @param partitionIndex the partition index
     * @param offset         the offset
     */
    TalariaContinuousStreamPartitionReader(String host, int port, String tableName, StructType schema, String partitionBy, int partitionIndex, long offset){
        this.offset = offset;
        this.nextOffset = offset;
        this.tableName = tableName;
        this.partitionBy = partitionBy;
        tc = new TalariaClient(host, port);
        getSplitID(this.offset);
        this.columns = util.getColumnsFromSchema(schema);
    }

    @Override
    public PartitionOffset getOffset() {
        return new TalariaContinuousStreamPartitionOffset(this.offset);
    }

    @Override
    public boolean next() {
        try {
            while (true) {

                nextOffset += 1;
                getSplitID(nextOffset);
                Thread.sleep(increment);
                rowsData = tc.getRows(splitID, columns);
                List<ColumnVector> cols = new ArrayList<>();
                int rowCount = rowsData.getRowCount();
                rowsData.getColumnsList().forEach(col -> cols.add(util.createColumnVector(rowCount, col)));
                ColumnVector[] cvs = cols.toArray(new ColumnVector[0]);
                ColumnarBatch batch = new ColumnarBatch(cvs, rowCount);
                splitID = rowsData.getNextToken();
                it = batch.rowIterator();
                if (it.hasNext()) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public InternalRow get() {
        return it.next();
    }

    @Override
    public void close() {
        tc.close();
    }

    private String createSortKeyUnBoundedFilter(Long start) {
        return "ingested_at >= " + start;
    }

    private void getSplitID(long start) {
        List<ByteString> splits = tc.getSplits(this.tableName, this.partitionBy, createSortKeyUnBoundedFilter(start));
        if (splits.size() == 0) {
            this.splitID = null;
        }else {
            this.splitID = splits.get(0);
        }
    }

}