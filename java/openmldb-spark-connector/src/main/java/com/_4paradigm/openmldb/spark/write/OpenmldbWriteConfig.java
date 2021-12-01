package com._4paradigm.openmldb.spark.write;

import com._4paradigm.openmldb.sdk.SdkOption;

import java.io.Serializable;

// Must serializable
public class OpenmldbWriteConfig implements Serializable {
    public String dbName, tableName, zkCluster, zkPath;

    public OpenmldbWriteConfig(String dbName, String tableName, SdkOption option) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.zkCluster = option.getZkCluster();
        this.zkPath = option.getZkPath();
        // TODO(hw): other configs in SdkOption
    }
}
