package com.talaria.spark.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;

/*
   TalariaScanBuilder: implements spark ScanBuilder and specifies the traits involded
   like pushdown filters e.t.c, at the moment we don't support any push down filters.
   This means it will be scanning all data without any filters applied(
   need to implement filter library at talaria to support this.)
   Although at query time, it just gets the required projections and applying the
   hashby and sortby filters.
 */
public class TalariaScanBuilder implements ScanBuilder, SupportsPushDownFilters {

    private final SparkSession spark;
    private final TalariaTable table;

    TalariaScanBuilder(SparkSession spark, TalariaTable table){
        this.spark = spark;
        this.table = table;
    }

    private Filter[] _pushedFilters;
    @Override
    public Scan build() {
        return new TalariaScan(spark, table);
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        _pushedFilters = filters;
        return filters;
    }

    @Override
    public Filter[] pushedFilters() {
        return _pushedFilters;
    }
}