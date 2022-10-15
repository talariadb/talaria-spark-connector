package com.talaria.spark.sql;


import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/*
    TalariaSource: implements SparkSql DataSourceRegister and TableProvider interfaces.
    DataSourceRegister simplifies spark to identify shortNames for a source
    eg: spark.read.format("talaria"), makes spark to look for meta-serviess which register
    DataSourceRegister in the resources/META-INF and this finding the required class
    by iterating over the classpath.
    If not found, please try giving the fully qualified class name
    eg: spark.read.format("com.talaria.spark.sql.TalariaSource")

    TableProvider implementation specifies spark on how to infer the schema of
    a talaria table and how to fetch table properties.

    TableScan will take over the controlflow from here.

 */
public class TalariaSource implements DataSourceRegister, TableProvider {

    @Override
    public String shortName() {
        return "talaria";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        Transform[] partitioning = {};
        return getTable(null, partitioning, options.asCaseSensitiveMap()).schema();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        ReadOptions rc = new ReadOptions(properties);
        return new TalariaTable(rc);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}