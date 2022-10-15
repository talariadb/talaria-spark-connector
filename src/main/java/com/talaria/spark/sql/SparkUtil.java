package com.talaria.spark.sql;

import com.talaria.protos.Column;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/* SparkUtil provides static utility methods to convert talaria gRPC encoded ORC bytes
   to spark/hive compatible ORC vectors.
   Used by the PartitionReader implementations.
 */
public class SparkUtil {

    public static List<String> getColumnsFromSchema(StructType schema) {
        return Arrays.asList(schema.fieldNames());
    }

    /*  createColumnVector: this method is kind of switch-case for various
        talaria datatypes, which creates the corresponding spark/hive ORC vectors.
     */
    public static ColumnVector createColumnVector(int rowCount, Column col){
        if (col.hasJson() || col.hasString()) {
            return createBytesVector(rowCount, col);
        }
        if (col.hasFloat64()){
            return createDoubleVector(rowCount, col);
        }
        if (col.hasInt32() || col.hasInt64() || col.hasBool()) {
            return createLongVector(rowCount, col);
        }
        if (col.hasTime()) {
            return createTimeStampVector(rowCount, col);
        }
        return null;
    }

    /* createTimeStampVector: converts talaria column of Timestamp to
       spark/hive Timestamp ORC vector
     */
    private static ColumnVector createTimeStampVector(int rowCount, Column col) {
        if (rowCount == 0) {
            return null;
        }
        TimestampColumnVector tcv = new TimestampColumnVector(rowCount);
        if (col.hasTime()) {
            for(int j = 0; j < rowCount; j++) {
                tcv.set(j, new Timestamp(col.getTime().getLongs(j)));
            }
        }
        return new OrcColumnVector(DataTypes.TimestampType, tcv);
    }

    /* createLongVector: converts talaria column of int64/32 and bool to
       spark/hive Long ORC vector
     */
    private static ColumnVector createLongVector(int rowCount, Column col) {
        if (rowCount == 0) {
            return null;
        }
        LongColumnVector lcv = new LongColumnVector(rowCount);
        if (col.hasInt64()) {
            for (int j=0; j<rowCount; j++) {
                lcv.vector[j] = col.getInt64().getLongs(j);
            }
        }
        if (col.hasInt32()) {
            for (int j=0; j<rowCount; j++) {
                lcv.vector[j] = col.getInt32().getInts(j);
            }
        }
        if (col.hasBool()) {
            for (int j=0; j<rowCount; j++) {
                lcv.vector[j] = col.getBool().getBools(j)?1:0;
            }
        }
        return new OrcColumnVector(DataTypes.LongType, lcv);
    }

    /* createDoubleVector: converts talaria column of float to
       spark/hive Double ORC vector
     */
    private static ColumnVector createDoubleVector(int rowCount, Column col) {
        if (rowCount == 0) {
            return null;
        }
        DoubleColumnVector dcv = new DoubleColumnVector(rowCount);
        for (int j=0;j<rowCount;j++){
            dcv.vector[j] = col.getFloat64().getDoubles(j);
        }
        return new OrcColumnVector(DataTypes.FloatType, dcv);
    }

    /* createDoubleVector: converts talaria column of string/json to
       spark/hive Bytes ORC vector
     */
    private static ColumnVector createBytesVector(int rowCount, Column col) {
        if (rowCount == 0) {
            return null;
        }
        BytesColumnVector bcv = new BytesColumnVector(rowCount);
        ByteBuffer buffer;
        List<Integer> sizes;
        List<Boolean> nulls;
        if (col.hasJson()) {
            buffer = col.getJson().getBytes().asReadOnlyByteBuffer();
            sizes = col.getJson().getSizesList();
            nulls = col.getJson().getNullsList();
        } else {
            buffer = col.getString().getBytes().asReadOnlyByteBuffer();
            sizes = col.getString().getSizesList();
            nulls = col.getString().getNullsList();
        }
        if (!buffer.hasRemaining()) {
            bcv.fillWithNulls();
            return new OrcColumnVector(DataTypes.ByteType, bcv);
        }
        // initBuffer is required to use the setVal method. setRef duplicates the data on heap
        // and increases memory footprint.
        bcv.initBuffer();
        for (int i=0; i<rowCount; i++) {
            if (nulls.get(i)) {
                bcv.isNull[i] = true;
                continue;
            }
            // Using ByteBuffer container to fetch byte array reduces the memory footprint.
            byte[] bytes = new byte[sizes.get(i)];
            buffer = buffer.get(bytes, 0, sizes.get(i));
            bcv.setVal(i, bytes);
        }

        return new OrcColumnVector(DataTypes.ByteType, bcv);
    }
}