package com.talaria.spark.sql;

import org.apache.spark.sql.connector.read.streaming.Offset;
import org.json.JSONObject;

/**
 * The type Talaria offset.
 */
public class TalariaOffset extends Offset {

    private final Long offset;

    /**
     * Instantiates a new Talaria offset.
     *
     * @param offset the offset
     */
    TalariaOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String json() {
        return "{ \"offset\" :  " + offset +"}";
    }

    /**
     * Gets offset.
     *
     * @return the offset
     */
    public Long getOffset() {
        return this.offset;
    }

    /**
     * From json talaria offset.
     *
     * @param json the json
     * @return the talaria offset
     */
    public static TalariaOffset fromJSON(String json){
        JSONObject jsonObject = new JSONObject(json);
        return new TalariaOffset(jsonObject.getLong("offset"));
    }
}