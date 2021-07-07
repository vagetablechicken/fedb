package com._4paradigm.dataimporter;

import com._4paradigm.fedb.ns.NS;
import com._4paradigm.fedb.ts.TS;
import com._4paradigm.hybridsql.fedb.DataType;
import com._4paradigm.hybridsql.fedb.DimMap;
import com._4paradigm.hybridsql.fedb.SQLInsertRow;
import com._4paradigm.hybridsql.fedb.SWIGTYPE_p_std__vectorT_std__pairT_std__string_unsigned_int_t_t;
import com._4paradigm.hybridsql.fedb.SWIGTYPE_p_std__vectorT_unsigned_long_long_t;
import com._4paradigm.hybridsql.fedb.Schema;
import com._4paradigm.hybridsql.fedb.sdk.SdkOption;
import com._4paradigm.hybridsql.fedb.sdk.SqlExecutor;
import com._4paradigm.hybridsql.fedb.sdk.impl.SqlClusterExecutor;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.MoreCollectors.onlyElement;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // src: file path
        // input way: config file, SQL "LOAD DATA INFILE", may needs hdfs sasl config
        logger.info("Start...");
        List<CSVRecord> rows = null;
        try {
//            Reader in = new FileReader("/home/huangwei/NYCTaxiDataset/train.csv"); // 192M
            Reader in = new FileReader("/home/huangwei/NYCTaxiDataset/train.csv.small"); // 9 rows
            CSVParser parser = new CSVParser(in, CSVFormat.EXCEL.withHeader());
            rows = parser.getRecords();
            // TODO(hw): pickup_datetime & dropoff_datetime need to transform to timestamp first
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        if (rows == null || rows.isEmpty()) {
            logger.warn("empty source file");
            return;
        }

        String dbName = "testdb";
        String tableName = "t1";

        SqlExecutor router = null;
        SdkOption option = new SdkOption();
        option.setZkCluster("172.24.4.55:6181");
        option.setZkPath("/onebox");
        try {
            router = new SqlClusterExecutor(option);

            // TODO(hw): check db exist
            router.executeDDL(dbName, "drop table " + tableName + ";");
            boolean ok = router.executeDDL(dbName, "create table " + tableName + "(\n" +
                    "id string,\n" +
                    "vendor_id int,\n" +
                    "pickup_datetime timestamp,\n" +
                    "dropoff_datetime timestamp,\n" +
                    "passenger_count int,\n" +
                    "pickup_longitude double,\n" +
                    "pickup_latitude double,\n" +
                    "dropoff_longitude double,\n" +
                    "dropoff_latitude double,\n" +
                    "store_and_fwd_flag string,\n" +
                    "trip_duration int,\n" +
                    "index(key=(vendor_id, passenger_count), ts=pickup_datetime),\n" +
                    "index(key=passenger_count, ts=pickup_datetime)\n" +
                    ");");
            if (!ok) {
                throw new RuntimeException("recreate table " + tableName + " failed");
            }
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return;
        }
        // reader, perhaps needs hdfs writer later
        // reader can read dir | file | *.xx?
        // support range? -> to use multi-threads
        int X = 8; // put_concurrency_limit default is 8
        logger.info("set thread num {}", X);

        logger.info("rows {}, peek {}", rows.size(), rows.isEmpty() ? "" : rows.get(0).toString());
        long startTime = System.currentTimeMillis();

//        insertImport(X, rows, router, dbName, tableName);

        // TODO(hw): What metadata does bulk load need?
        bulkLoad(X, rows, router, dbName, tableName);

        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;

        if (router != null) {
            router.close();
        }
        logger.info("End. Total time: {} ms", totalTime);
    }

    private static void insertImport(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        int rangeLen = (rows.size() + X) / X;
        List<Pair<Integer, Integer>> ranges = new ArrayList<>();
        int start = 0;
        // [left, right]
        for (int i = 0; i < X; ++i) {
            ranges.add(Pair.of(start, Math.min(start + rangeLen, rows.size())));
            start = start + rangeLen;
        }
        logger.info("ranges: {}", ranges);


        // We can ensure that the schema is match.
        List<Thread> threads = new ArrayList<>();
        for (Pair<Integer, Integer> range : ranges) {
            threads.add(new Thread(new InsertImporter(router, dbName, tableName, rows, range)));
        }
        // ETL?

        // dst: cluster name, db & table name
        // write to dst, by sdk, what about one row failed? ——关系到什么操作是原子的，以及如何组织插入，比如是否支持指定多路径对
        // 一张表，即使用户是散开写的，我们可以整合？如果用单条put的话，好像没啥意义。

        threads.forEach(Thread::start);

        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        });

    }

    private static void bulkLoad(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        // rows->list SQLInsertRow
        // for each Row
        // Use one record to generate insert place holder
        StringBuilder builder = new StringBuilder("insert into " + tableName + " values(");
        CSVRecord peekRecord = rows.get(0);
        for (int i = 0; i < peekRecord.size(); ++i) {
            builder.append((i == 0) ? "?" : ",?");
        }
        builder.append(");");
        String insertPlaceHolder = builder.toString();
        // TODO(hw): use this insertRow to create demo 为了少写代码

        logger.info("query zk");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("172.24.4.55:6181", retryPolicy);
        client.start();
        NS.TableInfo testTable = null;
        try {
            String tableInfoPath = "/onebox/table/db_table_data";
            List<String> tables = client.getChildren().forPath(tableInfoPath);
            if (tables == null) {
                logger.error("zk path {} get child failed.", tableInfoPath);
                return;
            }

            for (String tableId : tables) {
                byte[] tableInfo = client.getData().forPath(tableInfoPath + "/" + tableId);
                NS.TableInfo info = NS.TableInfo.parseFrom(tableInfo);
                logger.info(info.toString());
                if (info.getName().equals(tableName)) {
                    testTable = info;
                }
            }

            if (testTable == null) {
                logger.error("no table info of table {}", tableName);
                return;
            }
            // TODO(hw): test http request(proto type)
            for (NS.TablePartition partition : testTable.getTablePartitionList()) {
                logger.info("pid {}, {}", partition.getPid(), partition.getPartitionMetaList());
                NS.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(NS.PartitionMeta::getIsLeader).collect(onlyElement());
                TS.GetTableStatusRequest request = TS.GetTableStatusRequest.newBuilder().setTid(testTable.getTid()).setPid(partition.getPid()).build();

                HttpClient httpClient = new HttpClient();
                PostMethod postMethod = new PostMethod("http://" + leader.getEndpoint() + "/TabletServer/GetTableStatus");
                postMethod.addRequestHeader("Content-Type", "application/proto;charset=utf-8");
                postMethod.setRequestEntity(new ByteArrayRequestEntity(request.toByteArray()));
                httpClient.executeMethod(postMethod);

                TS.GetTableStatusResponse resp = TS.GetTableStatusResponse.parseFrom(postMethod.getResponseBodyAsStream());
                logger.info("get resp: {}", resp);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        assert testTable != null;
        long tid = testTable.getTid();
        Table<Long, Long, StringBuilder> dataMgr = HashBasedTable.create();

        List<TS.DataBlockInfo> dataBlockInfoList = new ArrayList<>();

        for (CSVRecord row : rows) {
            SQLInsertRow insertRow = router.getInsertRow(dbName, insertPlaceHolder);
            // TODO(hw): fulfill insertRow

            // TODO(hw): 拿insertRow的dims等信息，需要改sdk swig，但java复制步骤代码量不小，还是改sdk更容易。

            // SQLClusterRouter::PutRow
            DimMap dims = insertRow.GetDimensions(); // dims of pid
            SWIGTYPE_p_std__vectorT_unsigned_long_long_t tsDims = insertRow.GetTs();
            for (Map.Entry<Long, SWIGTYPE_p_std__vectorT_std__pairT_std__string_unsigned_int_t_t> entry : dims.entrySet()) { // TODO(hw): needs swig support
                Long pid = entry.getKey();
                // here has tid, pid, dimensions, ts_dimensions(maybe empty->cur_ts)
                // TODO(hw): check if tablets[pid] existed, this row should be sent to <tid, pid>->MemTable

                // TODO(hw):          if (ts_dimensions.empty()) {
                //                        ret = client->Put(tid, pid, cur_ts, row->GetRow(), kv.second, 1);
                //                    } else {
                //                        ret = client->Put(tid, pid, kv.second, row->GetTs(), row->GetRow(), 1);
                //                    }

                // TODO(hw): impl the second Put first
                //  TabletClient::Put tid, pid, entry.GetValue(), insertRow.GetTs(), insertRow.GetRow()(means data, String type)
                {
                    // TODO(hw):
                    //    ::fedb::api::PutRequest request;
                    //    request.set_value(value);
                    //    request.set_tid(tid);
                    //    request.set_pid(pid);
                    //    for (size_t i = 0; i < dimensions.size(); i++) {
                    //        ::fedb::api::Dimension* d = request.add_dimensions();
                    //        d->set_key(dimensions[i].first);
                    //        d->set_idx(dimensions[i].second);
                    //    }
                    //    for (size_t i = 0; i < ts_dimensions.size(); i++) {
                    //        ::fedb::api::TSDimension* d = request.add_ts_dimensions();
                    //        d->set_ts(ts_dimensions[i]);
                    //        d->set_idx(i);
                    //    }
                    //  assume that put request is here, how to insert the put request?
                    //  TabletImpl::Put 3种可能的put

                    // request.dimensions == row.dimensions == entry.GetValue()
                    TS.PutRequest request = TS.PutRequest.newBuilder().build(); // TODO(hw): to get fake dims & ts_dims
                    List<TS.Dimension> dimensions = request.getDimensionsList();
                    List<TS.TSDimension> ts_dimensions = request.getTsDimensionsList();

                    TS.BulkLoadInfoResponse indexInfo = TS.BulkLoadInfoResponse.newBuilder().build(); // TODO(hw): fake resp, request tablet server later
                    if (!dimensions.isEmpty()) {
                        // TODO(hw): CheckDimessionPut

                        if (!ts_dimensions.isEmpty()) {
                            // TODO(hw): 1 table->Put(request->dimensions(), request->ts_dimensions(), request->value());
                            // TODO(hw): when bulk loading, cannot AddIndex().
                            //  And MemTable::table_index_ may be modified by AddIndex()/Delete..., so we should get table_index_'s info from MemTable, to know the real status

                            Map<Integer, String> innerIndexKeyMap = new HashMap<>();
                            for (TS.Dimension dim : dimensions) {
                                innerIndexKeyMap.put(indexInfo.getInnerIndexPos(dim.getIdx()), dim.getKey());
                            }
                            AtomicInteger realRefCnt = new AtomicInteger();
                            innerIndexKeyMap.forEach((k, v) -> {
                                // TODO(hw): check idx valid
                                TS.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfo.getInnerIndex(k);
                                for (TS.BulkLoadInfoResponse.InnerIndexSt.IndexDef indexDef : innerIndex.getIndexDefList()) {
                                    // if has ts_col
                                    if (indexDef.getTsIdx() != -1) {
                                        boolean foundTs = ts_dimensions.stream().anyMatch(ts -> ts.getIdx() == indexDef.getTsIdx());
                                        if (foundTs && indexDef.getIsReady()) {
                                            realRefCnt.getAndIncrement();
                                        } else {
                                            logger.info("cannot find ts col.../ not ready...");
                                        }
                                    }
                                }
                            });

                            StringBuilder sb = dataMgr.get(tid, pid);
                            String rowData = insertRow.GetRow();
                            int head = sb.length();
                            sb.append(rowData);

                            long id = dataBlockInfoList.size();
                            // TODO(hw): realRefCnt == 0?
                            dataBlockInfoList.add(TS.DataBlockInfo.newBuilder().setRefCnt(realRefCnt.get()).setOffset(head).setLength(rowData.length()).build());

                            // TODO(hw): segment put use block id
                            innerIndexKeyMap.forEach((k, v) -> {
                                TS.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfo.getInnerIndex(k);
                                boolean needPut = innerIndex.getIndexDefList().stream().anyMatch(TS.BulkLoadInfoResponse.InnerIndexSt.IndexDef::getIsReady);
                                if (needPut) {
                                    int segIdx = 0;
                                    if (indexInfo.getSegCnt() > 1) {
                                        // TODO(hw): hash
                                    }
                                    // TODO(hw): segment[k][segIdx]->Put. In one segment, just two-level sorted array(desc)
                                    //  only in-memory first

                                    // v is pk, ts_dimension, data block
                                    // TODO(hw): needs Segment::ts_idx_map_
                                    //  index变化时new Segment会有很多情况，还是直接从MemTable处拿到每个Segment的情况最直接


                                }
                            });
                        } else {
                            // TODO(hw): 2 table->Put(request->time(), request->value(), request->dimensions());
                            //  inner_index_key_map is needed, calculated from ts_idx_vec
                        }

                    } else {
                        // TODO(hw): 3 table->Put(request->pk(), request->time(), request->value().c_str(), request->value().size());
                        //  Put 3 won't be used in this case. won't set pk,...
                        assert false;
                        // find seg_idx
                        int seg_idx = 0;
                        // if mem table seg_cnt_ > 1, hash
                        long id = dataBlockInfoList.size();
                        dataBlockInfoList.add(TS.DataBlockInfo.newBuilder().setRefCnt(1).build());
                        // segments_[0][seg_idx]->Put()
                    }
                }

//          In MemTable:
//          1. MemTable::Put -> gene real_ref_cnt, create DataBlockInfo & DataBlockId(append to one MemTable's Data Region, returns addr, len, blockId)
//                String data = insertRow.GetRow();
//                StringBuilder dataHolder = dataMgr.get(tid, pid);
//                int head = dataHolder.length();
//                dataHolder.append(data);
//                long id = dataBlockInfoList.size();
//                // TODO(hw): ref cnt
//                dataBlockInfoList.add(TS.DataBlockInfo.newBuilder().setOffset(head).setLength(data.length()).build());

//                // TODO(hw): rpc message都只有c版本，没有生产java，如果保持这样，那就得往sql cluster router里灌数据，让c++那边生成data/index region。这不符合以后要用spark的逻辑。
//                //  spark java能直接发rpc给tablet server


////              multi-threading insert into one MemTable dataHolder: needs lock?
////          2. find segments -> use blockId to do segment insertion
//                // One MemTable, how many inner_index? each inner_index contains two-level skip list
//                // like one PutRequest, TabletImpl::Put()
//                // dimensions_size==0? ts_dimensions_size==0?有三种MemTable::Put，都需要模拟
//
//                // needs MemTable::table_index_

            }
        }

        // TODO(hw): https://github.com/baidu/brpc-java/blob/master/brpc-java-examples/brpc-java-core-examples/src/main/java/com/baidu/brpc/example/standard/RpcClientTest.java
        //  有没有必要用java？


        // MemTable{Segments, Data region, blockId->DataBlockInfo}
        // so many MemTables
        // save to files? or just a rpc request? Maybe rpc requests is better in demo.
    }
}

class InsertImporter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InsertImporter.class);
    private final SqlExecutor router;
    private final String dbName;
    private final String tableName;
    private final List<CSVRecord> records;
    private final Pair<Integer, Integer> range;

    public InsertImporter(SqlExecutor router, String dbName, String tableName, List<CSVRecord> records, Pair<Integer, Integer> range) {
        this.router = router;
        this.dbName = dbName;
        this.tableName = tableName;
        this.records = records;
        this.range = range;
    }

    @Override
    public void run() {
        logger.info("Thread {} insert range {} ", Thread.currentThread().getId(), range);
        if (records.isEmpty()) {
            logger.warn("records is empty");
            return;
        }
        // Use one record to generate insert place holder
        StringBuilder builder = new StringBuilder("insert into " + tableName + " values(");
        CSVRecord peekRecord = records.get(0);
        for (int i = 0; i < peekRecord.size(); ++i) {
            builder.append((i == 0) ? "?" : ",?");
        }
        builder.append(");");
        String insertPlaceHolder = builder.toString();
        SQLInsertRow insertRow = router.getInsertRow(dbName, insertPlaceHolder);
        if (insertRow == null) {
            logger.warn("get insert row failed");
            return;
        }
        Schema schema = insertRow.GetSchema();

        // TODO(hw): record.getParser().getHeaderMap() check schema? In future, we may read multi files, so check schema in each worker.
        // What if the header of record is missing?
        List<String> stringCols = new ArrayList<>();
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            if (schema.GetColumnType(i) == DataType.kTypeString) {
                stringCols.add(schema.GetColumnName(i));
            }
        }
        for (int i = range.getLeft(); i < range.getRight(); i++) {
            // insert placeholder
            SQLInsertRow row = router.getInsertRow(dbName, insertPlaceHolder);
            CSVRecord record = records.get(i);
//            logger.info("{}", record.getParser().getHeaderMap());
            int strLength = stringCols.stream().mapToInt(col -> record.get(col).length()).sum();

            row.Init(strLength);
            boolean rowIsValid = true;
            for (int j = 0; j < schema.GetColumnCnt(); j++) {
                String v = record.get(schema.GetColumnName(j));
                DataType type = schema.GetColumnType(j);
                if (!appendToRow(v, type, row)) {
                    logger.warn("append to row failed, can't insert");
                    rowIsValid = false;
                }
            }
            if (rowIsValid) {
                router.executeInsert(dbName, insertPlaceHolder, row);
                // TODO(hw): retry
            }
        }
    }

    public boolean appendToRow(String v, DataType type, SQLInsertRow row) {
        // TODO(hw): true/false case sensitive? is null?
        // csv isSet?
        if (DataType.kTypeBool.equals(type)) {
            return row.AppendBool(v.equals("true"));
        } else if (DataType.kTypeInt16.equals(type)) {
            return row.AppendInt16(Short.parseShort(v));
        } else if (DataType.kTypeInt32.equals(type)) {
            return row.AppendInt32(Integer.parseInt(v));
        } else if (DataType.kTypeInt64.equals(type)) {
            return row.AppendInt64(Long.parseLong(v));
        } else if (DataType.kTypeFloat.equals(type)) {
            return row.AppendFloat(Float.parseFloat(v));
        } else if (DataType.kTypeDouble.equals(type)) {
            return row.AppendDouble(Double.parseDouble(v));
        } else if (DataType.kTypeString.equals(type)) {
            return row.AppendString(v);
        } else if (DataType.kTypeDate.equals(type)) {
            String[] parts = v.split("-");
            if (parts.length != 3) {
                return false;
            }
            long year = Long.parseLong(parts[0]);
            long mon = Long.parseLong(parts[1]);
            long day = Long.parseLong(parts[2]);
            return row.AppendDate(year, mon, day);
        } else if (DataType.kTypeTimestamp.equals(type)) {
            // TODO(hw): no need to support data time. Converting here is only for simplify. May fix later.
            if (v.contains("-")) {
                Timestamp ts = Timestamp.valueOf(v);
                return row.AppendTimestamp(ts.getTime()); // milliseconds
            }
            return row.AppendTimestamp(Long.parseLong(v));
        }
        return false;
    }
}