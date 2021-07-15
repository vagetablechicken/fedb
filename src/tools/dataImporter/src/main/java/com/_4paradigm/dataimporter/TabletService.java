package com._4paradigm.dataimporter;

import com._4paradigm.fedb.api.API;
import com.baidu.brpc.protocol.BrpcMeta;

public interface TabletService {
    // c++ serviceName doesn't contain the package name.
    @BrpcMeta(serviceName = "TabletServer", methodName = "GetTableStatus")
    API.GetTableStatusResponse getTableStatus(API.GetTableStatusRequest request);

    @BrpcMeta(serviceName = "TabletServer", methodName = "GetBulkLoadInfo")
    API.BulkLoadInfoResponse getBulkLoadInfo(API.BulkLoadInfoRequest request);

    @BrpcMeta(serviceName = "TabletServer", methodName = "BulkLoad")
    API.GeneralResponse bulkLoad(API.BulkLoadRequest request);
}
