package com._4paradigm.openmldb.spark.write;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OpenmldbDataWriter implements DataWriter<InternalRow> {
    private final int partitionId;
    private final long taskId;
    private SqlExecutor executor = null;
    private PreparedStatement preparedStatement = null;

    public OpenmldbDataWriter(int partitionId, long taskId) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        // TODO(hw): connect to openmldb
        SdkOption option = new SdkOption();
//    option.setZkCluster(zkAddress)
//    option.setZkPath(zkPath)
        option.setZkCluster("127.0.0.1:6181");
        option.setZkPath("/onebox");
        try {
            executor = new SqlClusterExecutor(option);
            preparedStatement = executor.getInsertPreparedStmt("db", "insert into t1 values(?);");
        } catch (SqlException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(InternalRow record) throws IOException {
        try {
            preparedStatement.setLong(1, 1L);
            preparedStatement.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        try {
            preparedStatement.executeBatch();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // TODO(hw): need return new WriterCommitMessageImpl(partitionId, taskId); ?
        return null;
    }

    @Override
    public void abort() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
