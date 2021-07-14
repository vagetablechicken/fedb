package com._4paradigm.dataimporter;

import com._4paradigm.fedb.api.API;
import com._4paradigm.fedb.nameserver.Nameserver;
import com._4paradigm.hybridsql.fedb.DataType;
import com._4paradigm.hybridsql.fedb.DimMap;
import com._4paradigm.hybridsql.fedb.PairStrInt;
import com._4paradigm.hybridsql.fedb.SQLInsertRow;
import com._4paradigm.hybridsql.fedb.Schema;
import com._4paradigm.hybridsql.fedb.VectorPairStrInt;
import com._4paradigm.hybridsql.fedb.VectorUint64;
import com._4paradigm.hybridsql.fedb.sdk.SdkOption;
import com._4paradigm.hybridsql.fedb.sdk.SqlExecutor;
import com._4paradigm.hybridsql.fedb.sdk.impl.SqlClusterExecutor;

import com.google.common.base.Preconditions;
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

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

        bulkLoadMulti(X, rows, router, dbName, tableName);

        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;

        router.close();

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
        Nameserver.TableInfo testTable = null;
        try {
            String tableInfoPath = "/onebox/table/db_table_data";
            List<String> tables = client.getChildren().forPath(tableInfoPath);
            if (tables == null) {
                logger.error("zk path {} get child failed.", tableInfoPath);
                return;
            }

            for (String tableId : tables) {
                byte[] tableInfo = client.getData().forPath(tableInfoPath + "/" + tableId);
                Nameserver.TableInfo info = Nameserver.TableInfo.parseFrom(tableInfo);
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
            for (Nameserver.TablePartition partition : testTable.getTablePartitionList()) {
                logger.info("pid {}, {}", partition.getPid(), partition.getPartitionMetaList());
                Nameserver.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(Nameserver.PartitionMeta::getIsLeader).collect(onlyElement());
                API.GetTableStatusRequest request = API.GetTableStatusRequest.newBuilder().setTid(testTable.getTid()).setPid(partition.getPid()).build();

                HttpClient httpClient = new HttpClient();
                PostMethod postMethod = new PostMethod("http://" + leader.getEndpoint() + "/TabletServer/GetTableStatus");
                postMethod.addRequestHeader("Content-Type", "application/proto;charset=utf-8");
                postMethod.setRequestEntity(new ByteArrayRequestEntity(request.toByteArray()));
                httpClient.executeMethod(postMethod);

                API.GetTableStatusResponse resp = API.GetTableStatusResponse.parseFrom(postMethod.getResponseBodyAsStream());
                logger.info("get resp: {}", resp);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        Preconditions.checkNotNull(testTable, "table is null");
        long tid = testTable.getTid();
        Table<Long, Long, BulkLoadRequest> tableRequestMgr = HashBasedTable.create();

        List<API.DataBlockInfo> dataBlockInfoList = new ArrayList<>();

        // TODO(hw): rpc message都只有c版本，没有生产java，如果保持这样，那就得往sql cluster router里灌数据，让c++那边生成data/index region。这不符合以后要用spark的逻辑。
        //  spark java能直接发rpc给tablet server, so we send BulkLoadRequest by this process.

        // TODO(hw): multi-thread how?
        for (CSVRecord row : rows) {
            SQLInsertRow insertRow = router.getInsertRow(dbName, insertPlaceHolder);
            // TODO(hw): fulfill insertRow

            // TODO(hw): 拿insertRow的dims等信息，需要改sdk swig，但java复制步骤代码量不小，还是改sdk更容易。

            // SQLClusterRouter::ExecuteInsertRow -> SQLClusterRouter::PutRow
            DimMap dims = insertRow.GetDimensions(); // dims of pid
            VectorUint64 rowTsDims = insertRow.GetTs();
            for (Map.Entry<Long, VectorPairStrInt> entry : dims.entrySet()) { // TODO(hw): needs swig support
                Long pid = entry.getKey();
                // here has tid, pid, dimensions, ts_dimensions(maybe empty->cur_ts)
                // TODO(hw): check if tablets[pid] existed, this row should be sent to <tid, pid>->MemTable

                // TODO(hw):          if (tsDims.empty()) { -> insertRow.GetTs(), not the PutRequest's ts_dimensions(it's the result of tsDims)
                //                        ret = client->Put(tid, pid, cur_ts, row->GetRow(), kv.second, 1);
                //                    } else {
                //                        ret = client->Put(tid, pid, kv.second, row->GetTs(), row->GetRow(), 1);
                //                    }
                VectorPairStrInt rowDims = entry.getValue();

                // TODO(hw): if(rowTsDims.isEmpty()) { time = currentTime; }

                // TODO(hw): to get fake dims & ts_dims
                //  rowDims -> request.dimensions()
                //  rowTsDims -> request.ts_dimensions() or request.time()(if rowTsDims is empty)
                API.PutRequest request = API.PutRequest.newBuilder().build();
                List<API.Dimension> dimensions = request.getDimensionsList();
                List<API.TSDimension> ts_dimensions = request.getTsDimensionsList();
                long time = request.getTime(); // may go to MemTable::Put 2 or MemTable::Put 3

                // TODO(hw): when bulk loading, cannot AddIndex().
                //  And MemTable::table_index_ may be modified by AddIndex()/Delete...,
                //  so we should get table_index_'s info from MemTable, to know the real status.
                //  And the status can't be changed until bulk lood finished.
                // TODO(hw): fake resp, request tablet server later
                API.BulkLoadInfoResponse indexInfo = API.BulkLoadInfoResponse.newBuilder().build();

                @Nullable BulkLoadRequest bulkLoadRequest = tableRequestMgr.get(tid, pid);
                if (bulkLoadRequest == null) {
                    bulkLoadRequest = new BulkLoadRequest(indexInfo); // built from BulkLoadInfoResponse
                    tableRequestMgr.put(tid, pid, bulkLoadRequest);
                }

                Map<Integer, String> innerIndexKeyMap = new HashMap<>();
                for (API.Dimension dim : dimensions) {
                    Preconditions.checkElementIndex(dim.getIdx(), indexInfo.getInnerIndexCount());
                    innerIndexKeyMap.put(indexInfo.getInnerIndexPos(dim.getIdx()), dim.getKey());
                }

                // Index Region insert, only use id
                int dataBlockId = dataBlockInfoList.size();
                // set the dataBlockInfo's ref count when index region insertion
                API.DataBlockInfo.Builder dataBlockInfoBuilder = API.DataBlockInfo.newBuilder();

                // TODO(hw): we use ExecuteInsert logic, so never call `table->Put(request->pk(), request->time(), request->value().c_str(), request->value().size());`
                //  即，不存在dims不存在却有ts dims的情况
                Preconditions.checkState(!dimensions.isEmpty());

                // TODO(hw): CheckDimessionPut

                AtomicInteger realRefCnt = new AtomicInteger();
                // 1. if ts_dimensions is empty, we will put data into `ready Index` without checking.
                //      But we'll check the Index whether has the ts column. Mismatch meta returns false.
                // 2. if ts_dimensions is not empty, we will find the corresponding ts_dimensions to put data. If can't find, continue.
                innerIndexKeyMap.forEach((k, v) -> {
                    // TODO(hw): check idx valid
                    API.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfo.getInnerIndex(k);
                    for (API.BulkLoadInfoResponse.InnerIndexSt.IndexDef indexDef : innerIndex.getIndexDefList()) {
                        //
                        if (ts_dimensions.isEmpty() && indexDef.getTsIdx() != -1) {
                            throw new RuntimeException("IndexStatus has the ts column, but InsertRow doesn't have ts_dimensions.");
                        }

                        if (!ts_dimensions.isEmpty()) {
                            // just continue
                            if (indexDef.getTsIdx() == -1 || ts_dimensions.stream().noneMatch(ts -> ts.getIdx() == indexDef.getTsIdx())) {
                                continue;
                            }
                            // TODO(hw): But there may be another question.
                            //  if we can't find here, but indexDef is ready, we may put in the next phase.
                            //  (foundTs is not corresponding to the put index, we can't ensure that?)
                        }

                        if (indexDef.getIsReady()) {
                            realRefCnt.incrementAndGet();
                        }
                    }
                });

                // if no ts_dimensions, it's ok to warp the current time into ts_dimensions.
                List<API.TSDimension> tsDimsWrap = ts_dimensions;
                if (ts_dimensions.isEmpty()) {
                    tsDimsWrap = Collections.singletonList(API.TSDimension.newBuilder().setTs(time).build());
                }
                for (Map.Entry<Integer, String> idx2key : innerIndexKeyMap.entrySet()) {
                    Integer idx = idx2key.getKey();
                    String key = idx2key.getValue();
                    API.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfo.getInnerIndex(idx);
                    boolean needPut = innerIndex.getIndexDefList().stream().anyMatch(API.BulkLoadInfoResponse.InnerIndexSt.IndexDef::getIsReady);
                    if (needPut) {
                        int segIdx = 0;
                        if (indexInfo.getSegCnt() > 1) {
                            // TODO(hw): hash
                        }
                        // TODO(hw): segment[k][segIdx]->Put. only in-memory first.
                        SegmentDataMap segment = bulkLoadRequest.segmentDataMaps.get(idx).get(segIdx);

                        // TODO(hw): void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row)
                        segment.Put(key, tsDimsWrap, dataBlockId);
                    }
                }

                // TODO(hw): if success, add data & info
                StringBuilder sb = bulkLoadRequest.dataBlock;
                String rowData = insertRow.GetRow();
                int head = sb.length();
                sb.append(rowData);
                dataBlockInfoBuilder.setRefCnt(realRefCnt.get()).setOffset(head).setLength(rowData.length());
                dataBlockInfoList.add(dataBlockInfoBuilder.build());
                // TODO(hw): multi-threading insert into one MemTable dataHolder: needs lock?
            } // One <tid, pid> Put for One Row End
        }

        // TODO(hw): https://github.com/baidu/brpc-java/blob/master/brpc-java-examples/brpc-java-core-examples/src/main/java/com/baidu/brpc/example/standard/RpcClientTest.java
        //  有没有必要用java？


        // TODO(hw): so many MemTables
        //  save to files? or just a rpc request? Maybe rpc requests is better in demo.
    }

    // TODO(hw): 流程优先，所以先假设可以开MemTable个线程，分别负责。之后再考虑线程数有限的情况。
    private static void bulkLoadMulti(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        // MemTable.size() threads BulkLoadGenerator

        logger.info("query zk for table meta data");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("172.24.4.55:6181", retryPolicy);
        client.start();
        Nameserver.TableInfo testTable = null;
        try {
            String tableInfoPath = "/onebox/table/db_table_data";
            List<String> tables = client.getChildren().forPath(tableInfoPath);
            Preconditions.checkNotNull(tables, "zk path {} get child failed.", tableInfoPath);

            for (String tableId : tables) {
                byte[] tableInfo = client.getData().forPath(tableInfoPath + "/" + tableId);
                Nameserver.TableInfo info = Nameserver.TableInfo.parseFrom(tableInfo);
                logger.info(info.toString());
                if (info.getName().equals(tableName)) {
                    testTable = info;
                }
            }
            Preconditions.checkNotNull(testTable, "no table info of table {}", tableName);

            for (Nameserver.TablePartition partition : testTable.getTablePartitionList()) {
                logger.info("pid {}, {}", partition.getPid(), partition.getPartitionMetaList());
                Nameserver.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(Nameserver.PartitionMeta::getIsLeader).collect(onlyElement());
                API.GetTableStatusRequest request = API.GetTableStatusRequest.newBuilder().setTid(testTable.getTid()).setPid(partition.getPid()).build();

                HttpClient httpClient = new HttpClient();
                PostMethod postMethod = new PostMethod("http://" + leader.getEndpoint() + "/TabletServer/GetTableStatus");
                postMethod.addRequestHeader("Content-Type", "application/proto;charset=utf-8");
                postMethod.setRequestEntity(new ByteArrayRequestEntity(request.toByteArray()));
                // TODO(hw): how to add attachment? http/h2协议中附件对应message body，没有request attachment的位置了。。。只呢用brpc-java版？
                httpClient.executeMethod(postMethod);

                API.GetTableStatusResponse resp = API.GetTableStatusResponse.parseFrom(postMethod.getResponseBodyAsStream());
                logger.info("get resp: {}", resp);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        // When bulk loading, cannot AddIndex().
        //  And MemTable::table_index_ may be modified by AddIndex()/Delete...,
        //  so we should get table_index_'s info from MemTable, to know the real status.
        //  And the status can't be changed until bulk lood finished.

        // TODO(hw): fake resp, request tablet server later!!
        API.BulkLoadInfoResponse indexInfo = API.BulkLoadInfoResponse.newBuilder().build();

        List<Thread> threads = new ArrayList<>();
        Map<Integer, BulkLoadGenerator> generators = new HashMap<>();
        testTable.getTablePartitionList().forEach(tablePartition -> {
            Nameserver.PartitionMeta partitionLeader = tablePartition.getPartitionMetaList().stream().filter(Nameserver.PartitionMeta::getIsLeader).collect(onlyElement());

            BulkLoadGenerator generator = new BulkLoadGenerator(tablePartition.getPid(), indexInfo, partitionLeader.getEndpoint());
            generators.put(tablePartition.getPid(), generator);
            threads.add(new Thread(generator));
        });
        logger.info("create {} generators", generators.size());

        StringBuilder builder = new StringBuilder("insert into " + tableName + " values(");
        CSVRecord peekRecord = rows.get(0);
        for (int i = 0; i < peekRecord.size(); ++i) {
            builder.append((i == 0) ? "?" : ",?");
        }
        builder.append(");");
        String insertPlaceHolder = builder.toString();
        // Try get one insert row to generate stringCols
        SQLInsertRow insertRowTmp = router.getInsertRow(dbName, insertPlaceHolder);
        Preconditions.checkNotNull(insertRowTmp);
        Schema schema = insertRowTmp.GetSchema();
        List<String> stringCols = InsertImporter.getStringColumnsFromSchema(schema);

        for (CSVRecord record : rows) {
            SQLInsertRow row = router.getInsertRow(dbName, insertPlaceHolder);

            // TODO(hw): fulfill insertRow
//            logger.info("{}", record.getParser().getHeaderMap());
            int strLength = stringCols.stream().mapToInt(col -> record.get(col).length()).sum();
            row.Init(strLength);

            boolean rowIsValid = true;
            for (int j = 0; j < schema.GetColumnCnt(); j++) {
                String v = record.get(schema.GetColumnName(j));
                DataType type = schema.GetColumnType(j);
                if (!InsertImporter.appendToRow(v, type, row)) {
                    logger.warn("append to row failed, can't insert");
                    rowIsValid = false;
                    break;
                }
            }
            if (!rowIsValid || !row.Build()) {
                // TODO(hw): How to handle one invalid row?
                logger.error("invalid row, exit for simplicity");
                return;
            }

            // SQLClusterRouter::ExecuteInsertRow -> SQLClusterRouter::PutRow
            DimMap dims = row.GetDimensions(); // dims of pid
            for (Long pid : dims.keySet()) {
                try {
                    // nameserver pid is int
                    generators.get(pid.intValue()).feed(row);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error("feed error, skip this row or retry, or fail all?");
                }
            }
        }

        generators.forEach((integer, bulkLoadGenerator) -> bulkLoadGenerator.shutDown());
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (generators.values().stream().anyMatch(BulkLoadGenerator::hasInternalError)) {
            logger.error("BulkLoad failed...");
            return;
        }

        // TODO(hw): rpc send, multi thread send?
        generators.values().forEach(bulkLoadGenerator -> {
            if (!bulkLoadGenerator.sendRequest()) {
                logger.error("send bulk load request error.");
            }
        });

        // TODO(hw): get statistics from generators
        logger.info("BulkLoad finished.");
    }

    public static class SegmentDataMap {
        // TODO(hw): id ?
        final int tsCnt;
        final Map<Integer, Integer> tsIdxMap;
        public int ONE_IDX = 0;
        public int NO_IDX = 0;

        // TODO(hw): SegmentIndexMap treeMap, can do reverse iter? comparator1, Slice::compare, comparator2, TimeComparator
        Map<String, List<Map<Long, Integer>>> keyEntries = new TreeMap<>();

        public SegmentDataMap(int tsCnt, Map<Integer, Integer> tsIdxMap) {
            this.tsCnt = tsCnt;
            this.tsIdxMap = tsIdxMap; // could be empty
        }

        public void Put(String key, int idxPos, Long time, Integer id) {
            List<Map<Long, Integer>> entryList = keyEntries.getOrDefault(key, new ArrayList<>());
            if (entryList.isEmpty()) {
                for (int i = 0; i < tsCnt; i++) {
                    entryList.add(new TreeMap<>());// TODO(hw): comparator?
                }
            }
            entryList.get(idxPos).put(time, id);
        }

        public void Put(String key, List<API.TSDimension> tsDimensions, Integer dataBlockId) {
            if (tsDimensions.isEmpty()) {
                return;
            }
            if (tsCnt == 1) {
                if (tsDimensions.size() == 1) {
                    Put(key, this.NO_IDX, tsDimensions.get(0).getTs(), dataBlockId);
                } else {
                    // tsCnt == 1 & has tsIdxMap, so tsIdxMap only has one element.
                    Preconditions.checkArgument(tsIdxMap.size() == 1);
                    Integer tsIdx = tsIdxMap.keySet().stream().collect(onlyElement());
                    API.TSDimension needPutIdx = tsDimensions.stream().filter(tsDimension -> tsDimension.getIdx() == tsIdx).collect(onlyElement());
                    Put(key, this.ONE_IDX, needPutIdx.getTs(), dataBlockId);
                }
            } else {
                // tsCnt != 1, KeyEntry array for one key

                for (API.TSDimension tsDimension : tsDimensions) {
                    Integer pos = tsIdxMap.get(tsDimension.getIdx());
                    if (pos == null) {
                        continue;
                    }
                    Put(key, pos, tsDimension.getTs(), dataBlockId);
                }
            }
        }
        // TODO(hw): serialize to `message Segment`
    }

    public static class BulkLoadRequest {
        public List<List<SegmentDataMap>> segmentDataMaps;
        public StringBuilder dataBlock = new StringBuilder();
        public List<API.DataBlockInfo> dataBlockInfoList = new ArrayList<>();

        public BulkLoadRequest(API.BulkLoadInfoResponse bulkLoadInfo) {
            segmentDataMaps = new ArrayList<>();

            bulkLoadInfo.getInnerSegmentsList().forEach(
                    innerSegments -> {
                        List<SegmentDataMap> segments = new ArrayList<>();
                        innerSegments.getSegmentList().forEach(
                                segmentInfo -> {
                                    // ts_idx_map array to map, proto2 doesn't support map.
                                    Map<Integer, Integer> tsIdxMap = segmentInfo.getTsIdxMapList().stream().collect(Collectors.toMap(
                                            API.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getKey,
                                            API.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getValue)); // can't tolerate dup key
                                    int tsCnt = segmentInfo.getTsCnt();
                                    SegmentDataMap segment = new SegmentDataMap(tsCnt, tsIdxMap);
                                    segments.add(new SegmentDataMap((segmentInfo.getTsCnt()), tsIdxMap));
                                }
                        );
                        this.segmentDataMaps.add(segments);
                    }
            );
        }
        // TODO(hw): serialize to API.BulkLoadRequest

    }

    // TODO(hw): murmurhash2 java version
    static long hash(String key, long seed) {
        long m = 0x5bd1e995;
        long r = 24;
        long len = key.length();
        long h = seed ^ len;
        return h;
    }
}

class BulkLoadGenerator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoadGenerator.class);
    private final int pid;
    private final BlockingQueue<SQLInsertRow> queue;
    private final long pollTimeout;
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private boolean internalErrorOcc = false;
    private final API.BulkLoadInfoResponse indexInfo; // TODO(hw): not a good name
    private final Main.BulkLoadRequest bulkLoadRequest;
    private final List<API.DataBlockInfo> dataBlockInfoList = new ArrayList<>();
    private final String endpoint;

    public BulkLoadGenerator(int pid, API.BulkLoadInfoResponse indexInfo, String endpoint) {
        this.pid = pid;
        this.queue = new ArrayBlockingQueue<SQLInsertRow>(1000);
        this.pollTimeout = 100;
        this.indexInfo = indexInfo;
        this.bulkLoadRequest = new Main.BulkLoadRequest(indexInfo); // built from BulkLoadInfoResponse
        this.endpoint = endpoint;
    }

    @Override
    public void run() {
        logger.info("Thread {} for MemTable(pid {})", Thread.currentThread().getId(), pid);
        try {
            // exit statement: shutdown and no element in queue, or internal exit
            while (!shutdown.get() || !this.queue.isEmpty()) {
                SQLInsertRow row = this.queue.poll(this.pollTimeout, TimeUnit.MILLISECONDS);
                if (row != null) {
                    // TODO(hw): get dims by pid, needs swig support
                    //  拿insertRow的dims等信息，需要改sdk swig，但java复制步骤代码量不小，还是改sdk更容易。
                    VectorPairStrInt dimensions = row.GetDimensions().get((long) this.pid);
                    // tsDimensions[idx] = ts, we convert it to API.TSDimensions for simplicity
                    VectorUint64 tsDimVec = row.GetTs();
                    List<API.TSDimension> tsDimensions = new ArrayList<>();
                    for (int i = 0; i < tsDimVec.size(); i++) {
                        tsDimensions.add(API.TSDimension.newBuilder().setIdx(i).setTs(tsDimVec.get(i)).build());
                    }
                    long time = System.currentTimeMillis();

                    Map<Integer, String> innerIndexKeyMap = new HashMap<>();
                    // PairStrInt: str-key, int-idx. == message Dimension
                    for (PairStrInt dim : dimensions) {
                        String key = dim.getFirst();
                        long idx = dim.getSecond();
                        // TODO(hw): idx is uint32, but info size is int
                        Preconditions.checkElementIndex((int) idx, indexInfo.getInnerIndexCount());
                        innerIndexKeyMap.put(indexInfo.getInnerIndexPos((int) idx), key);
                    }

                    // Index Region insert, only use id
                    int dataBlockId = dataBlockInfoList.size();
                    // set the dataBlockInfo's ref count when index region insertion
                    API.DataBlockInfo.Builder dataBlockInfoBuilder = API.DataBlockInfo.newBuilder();

                    // TODO(hw): we use ExecuteInsert logic, so never call `table->Put(request->pk(), request->time(), request->value().c_str(), request->value().size());`
                    //  即，不存在dims不存在却有ts dims的情况
                    Preconditions.checkState(!dimensions.isEmpty());

                    // TODO(hw): CheckDimessionPut

                    AtomicInteger realRefCnt = new AtomicInteger();
                    // 1. if tsDimensions is empty, we will put data into `ready Index` without checking.
                    //      But we'll check the Index whether has the ts column. Mismatch meta returns false.
                    // 2. if tsDimensions is not empty, we will find the corresponding tsDimensions to put data. If can't find, continue.
                    innerIndexKeyMap.forEach((k, v) -> {
                        // TODO(hw): check idx valid
                        API.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfo.getInnerIndex(k);
                        for (API.BulkLoadInfoResponse.InnerIndexSt.IndexDef indexDef : innerIndex.getIndexDefList()) {
                            //
                            if (tsDimensions.isEmpty() && indexDef.getTsIdx() != -1) {
                                throw new RuntimeException("IndexStatus has the ts column, but InsertRow doesn't have tsDimensions.");
                            }

                            if (!tsDimensions.isEmpty()) {
                                // just continue
                                if (indexDef.getTsIdx() == -1 || tsDimensions.stream().noneMatch(ts -> ts.getIdx() == indexDef.getTsIdx())) {
                                    continue;
                                }
                                // TODO(hw): But there may be another question.
                                //  if we can't find here, but indexDef is ready, we may put in the next phase.
                                //  (foundTs is not corresponding to the put index, we can't ensure that?)
                            }

                            if (indexDef.getIsReady()) {
                                realRefCnt.incrementAndGet();
                            }
                        }
                    });

                    // if no tsDimensions, it's ok to warp the current time into tsDimensions.
                    List<API.TSDimension> tsDimsWrap = tsDimensions;
                    if (tsDimensions.isEmpty()) {
                        tsDimsWrap = Collections.singletonList(API.TSDimension.newBuilder().setTs(time).build());
                    }
                    for (Map.Entry<Integer, String> idx2key : innerIndexKeyMap.entrySet()) {
                        Integer idx = idx2key.getKey();
                        String key = idx2key.getValue();
                        API.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfo.getInnerIndex(idx);
                        boolean needPut = innerIndex.getIndexDefList().stream().anyMatch(API.BulkLoadInfoResponse.InnerIndexSt.IndexDef::getIsReady);
                        if (needPut) {
                            int segIdx = 0;
                            if (indexInfo.getSegCnt() > 1) {
                                // TODO(hw): hash, use random here util java hash finished.
                                segIdx = new Random().nextInt();
                            }
                            // TODO(hw): segment[k][segIdx]->Put. only in-memory first.
                            Main.SegmentDataMap segment = bulkLoadRequest.segmentDataMaps.get(idx).get(segIdx);

                            // void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row)
                            segment.Put(key, tsDimsWrap, dataBlockId);
                        }
                    }

                    // TODO(hw): if success, add data & info
                    StringBuilder sb = bulkLoadRequest.dataBlock;
                    String rowData = row.GetRow();
                    int head = sb.length();
                    sb.append(rowData);
                    dataBlockInfoBuilder.setRefCnt(realRefCnt.get()).setOffset(head).setLength(rowData.length());
                    dataBlockInfoList.add(dataBlockInfoBuilder.build());
                    // TODO(hw): multi-threading insert into one MemTable dataHolder: needs lock?

                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void feed(SQLInsertRow row) throws InterruptedException {
        this.queue.put(row); // blocking put
    }

    public void shutDown() {
        this.shutdown.set(true);
    }

    public boolean hasInternalError() {
        return internalErrorOcc;
    }

    public boolean sendRequest() {
        // TODO(hw): request -> pb
        // TODO(hw): send rpc
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
        List<String> stringCols = getStringColumnsFromSchema(schema);
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

    public static List<String> getStringColumnsFromSchema(Schema schema) {
        List<String> stringCols = new ArrayList<>();
        for (int i = 0; i < schema.GetColumnCnt(); i++) {
            if (schema.GetColumnType(i) == DataType.kTypeString) {
                // TODO(hw): what if data don't have column names?
                stringCols.add(schema.GetColumnName(i));
            }
        }
        return stringCols;
    }

    public static boolean appendToRow(String v, DataType type, SQLInsertRow row) {
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