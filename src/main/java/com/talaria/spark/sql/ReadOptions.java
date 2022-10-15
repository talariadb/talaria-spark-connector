package com.talaria.spark.sql;

import java.util.Map;

/**
   ReadOptions specify different spark conf options
   when invoked using spark.read you can provide the
   following options.
 */
public class ReadOptions {
    private final String domain;
    private final Integer port;
    private final String schema;
    private final String table;
    private final String partitionFilter;
    private final String checkpointLocation;
    private final Long fromTimestamp;
    private final Long untilTimestamp;

    public String getDomain() {
        return domain;
    }

    public Integer getPort() {
        return port;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getPartitionFilter() {
        return partitionFilter;
    }

    public Long getFromTimestamp() {
        return fromTimestamp;
    }

    public Long getUntilTimestamp() {
        return untilTimestamp;
    }

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public ReadOptions(Map<String, String> options) {
        /* domain: specifies talaria cluster domain or any one of the nodes'
                   ip address as it internally uses gossip protocol across the cluster.
         */
        String DOMAIN = "domain";
        /* port: port of talaria nodes' grpc listeners. */
        String PORT = "port";
        /* table: table to query on. */
        String TABLE = "table";
        /* schema: schema to find table. */
        String SCHEMA = "schema";
        /* partitionFilter: filter string to specify talaria table's Hashby
           eg: "relay.outcome" will be converted to 'event(the Hashby column) == "relay.outcome"'
           based on the table's hashby property.
         */
        String PARTITIONFILTER = "partitionFilter";
        /* checkpointLocation: load the last successful batch/micro-batch checkpoint
           from this location. Typically used for fault-tolerance and crash-resilience.
           If no file was found at checkpoint location, the operations take current-time
           in millis and proceed. If specified, offsets are written to the specific
           location, else the execution remains stateless.
        */
        String CHECKPOINTLOCATION = "checkpointLocation";
        // TODO: this configuration parameters are valid for batch loads only.
        /* fromTimestamp: specifies the ts value in epoch seconds to begin the query
           from. Internally it uses >= on this value which means it forms an inclusive bound.
           The filter is applied by default on the sortby column.
        */
        String FROMTS = "fromTimestamp";
        // TODO: this configuration parameters are valid for batch loads only.
        /* untilTimestamp: specifies the ts value in epoch seconds to end the query
            till. Internally it uses < on this value which means it forms an exclusive bound.
            The filter is applied by default on the sortby column.
         */
        String UNTILTS = "untilTimestamp";

        this.domain = options.get(DOMAIN);
        this.port = Integer.valueOf(options.get(PORT));
        this.table = options.get(TABLE);
        this.schema = options.get(SCHEMA);
        this.partitionFilter = options.get(PARTITIONFILTER);
        this.fromTimestamp = parseTimeBoundOptions(options.get(FROMTS), true);
        this.untilTimestamp = parseTimeBoundOptions(options.get(UNTILTS), false);
        this.checkpointLocation = options.get(CHECKPOINTLOCATION);
    }

    private Long parseTimeBoundOptions(String epochStr, Boolean lower){
        long value = 0L;
        try {
            value = Long.parseLong(epochStr);
        }
        catch (NumberFormatException exception){
           if (!lower){
               value = Long.MAX_VALUE;
           }
        }
        return value;
    }
}