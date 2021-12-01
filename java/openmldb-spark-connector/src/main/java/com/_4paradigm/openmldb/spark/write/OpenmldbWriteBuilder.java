package com._4paradigm.openmldb.spark.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class OpenmldbWriteBuilder implements WriteBuilder {
    private final OpenmldbWriteConfig config;
    private final LogicalWriteInfo info;

    public OpenmldbWriteBuilder(OpenmldbWriteConfig config, LogicalWriteInfo info) {
        this.config = config;
        this.info = info;
    }

    @Override
    public BatchWrite buildForBatch() {
        return new OpenmldbBatchWrite(config, info);
    }
}
