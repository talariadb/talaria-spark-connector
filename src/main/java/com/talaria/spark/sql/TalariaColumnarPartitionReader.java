package com.talaria.spark.sql;

import com.google.protobuf.ByteString;
import com.talaria.client.TalariaClient;
import com.talaria.protos.GetRowsResponse;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.List;

/*
    TalariaColumnarPartitionReader: implements the spark PartitionReader for columnar batch reads.
 */
public class TalariaColumnarPartitionReader implements PartitionReader<ColumnarBatch> {

    TalariaClient tc;
    ByteString splitID;
    List<String> columns;
    GetRowsResponse rowsData;

    TalariaColumnarPartitionReader(String host, int port, String tableName, StructType schema, String tSchema, String hashBy, String sortBy, String partitionFilter, Long start, Long end) {
        // offload maxInboundMsgSize to SparkSession config  if possible
        tc = new TalariaClient(host, port, 32*1024*1024);
        List<ByteString> splits = tc.getSplits(tSchema, tableName, createPartitionFilter(hashBy, partitionFilter), createSortKeyBoundedFilter(sortBy, start, end));
        if (splits.size() == 0) {
            this.splitID = null;
        }else {
            this.splitID = splits.get(0);
        }
        this.columns = SparkUtil.getColumnsFromSchema(schema);
    }

    @Override
    public boolean next() {
        return splitID != null && !splitID.isEmpty();
    }

    @Override
    public ColumnarBatch get() {
        // offload maxMsgSize to SparkSession config  if possible
        rowsData = tc.getRows(splitID, columns, 32*1024*1024);
        List<ColumnVector> cols = new ArrayList<>();
        int rowCount = rowsData.getRowCount();
        rowsData.getColumnsList().forEach(col -> cols.add(SparkUtil.createColumnVector(rowCount, col)));
        ColumnVector[] cvs = cols.toArray(new ColumnVector[0]);
        ColumnarBatch batch = new ColumnarBatch(cvs, rowCount);
        splitID = rowsData.getNextToken();
        return batch;
    }

    @Override
    public void close() {
        tc.close();
    }

    private String createPartitionFilter(String hashBy, String partitionFilter) {
        return String.format("%s == '%s'", hashBy, partitionFilter);
    }

    private String createSortKeyBoundedFilter(String sortBy, Long start, Long end) {
        return String.format("%s >= %d && %s < %s", sortBy, start, sortBy, end);
    }
}