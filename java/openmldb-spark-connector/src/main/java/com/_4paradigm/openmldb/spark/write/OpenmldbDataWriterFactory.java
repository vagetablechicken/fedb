package com._4paradigm.openmldb.spark.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class OpenmldbDataWriterFactory implements DataWriterFactory {
    private final OpenmldbWriteConfig config;

    public OpenmldbDataWriterFactory(OpenmldbWriteConfig config) {
        this.config = config;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new OpenmldbDataWriter(config, partitionId, taskId);
    }
}
