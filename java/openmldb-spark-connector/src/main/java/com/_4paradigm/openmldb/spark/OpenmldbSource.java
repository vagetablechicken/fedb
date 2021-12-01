package com._4paradigm.openmldb.spark;

import com._4paradigm.openmldb.sdk.SdkOption;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class OpenmldbSource implements TableProvider, DataSourceRegister {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        // TODO(hw): extractOptions => openmldb params, store in this.params is ok
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        // get name from this.params
        SdkOption option = new SdkOption();
        //    option.setZkCluster(zkAddress)
//    option.setZkPath(zkPath)
        option.setZkCluster("127.0.0.1:6181");
        option.setZkPath("/onebox");
        return new OpenmldbTable("db", "t1", option);
    }

    @Override
    public String shortName() {
        return "openmldb";
    }
}
