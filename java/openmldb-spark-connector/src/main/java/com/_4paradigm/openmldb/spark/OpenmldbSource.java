package com._4paradigm.openmldb.spark;

import com._4paradigm.openmldb.sdk.SdkOption;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class OpenmldbSource implements TableProvider, DataSourceRegister {
    private final String DB = "db";
    private final String TABLE = "table";
    private final String ZK_CLUSTER = "zkCluster";
    private final String ZK_PATH = "zkPath";

    private String dbName;
    private String tableName;
    private SdkOption option = null;

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        Preconditions.checkNotNull(dbName = options.get(DB));
        Preconditions.checkNotNull(tableName = options.get(TABLE));

        String zkCluster = options.get(ZK_CLUSTER);
        String zkPath = options.get(ZK_PATH);
        Preconditions.checkNotNull(zkCluster);
        Preconditions.checkNotNull(zkPath);
        option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        String timeout = options.get("sessionTimeout");
        if (timeout != null) {
            option.setSessionTimeout(Integer.parseInt(timeout));
        }
        timeout = options.get("requestTimeout");
        if (timeout != null) {
            option.setRequestTimeout(Integer.parseInt(timeout));
        }
        String debug = options.get("debug");
        if (debug != null) {
            option.setEnableDebug(Boolean.valueOf(debug));
        }

        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new OpenmldbTable(dbName, tableName, option);
    }

    @Override
    public String shortName() {
        return "openmldb";
    }
}
