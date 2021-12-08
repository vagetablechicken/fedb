package com._4paradigm.hybridse;

import com._4paradigm.hybridse.sdk.SqlEngine;
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException;
import com._4paradigm.hybridse.type.TypeOuterClass;
import com._4paradigm.hybridse.vm.Engine;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HybridSeLibraryTest {
    @Test
    public void initCoreTest() {
        Assert.assertFalse(HybridSeLibrary.isInitialized());
        HybridSeLibrary.initCore();
        Assert.assertTrue(HybridSeLibrary.isInitialized());
        Engine.InitializeGlobalLLVM();
        try {
            SqlEngine engine = new SqlEngine("", TypeOuterClass.Database.newBuilder().build());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
        }
        SdkOption option = new SdkOption();
        option.setZkCluster("");
        option.setZkPath("/");
        try {
            SqlClusterExecutor executor = new SqlClusterExecutor(option);
        } catch (SqlException e) {
            e.printStackTrace();
        }

    }
}
