package com._4paradigm.dataimporter;

import com._4paradigm.fedb.api.API;
import com._4paradigm.fedb.nameserver.Nameserver;
import com._4paradigm.fedb.type.Type;
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

import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.protobuf.ByteString.copyFromUtf8;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // src: file path
        // input way: config file, SQL "LOAD DATA INFILE", may needs hdfs sasl config
        logger.info("Start...");
        List<CSVRecord> rows = null;
        try {
//            Reader in = new FileReader("/home/huangwei/NYCTaxiDataset/train.csv"); // 192M
            Reader in = new FileReader("/home/huangwei/NYCTaxiDataset/train.csv.debug"); // debug
            CSVParser parser = new CSVParser(in, CSVFormat.EXCEL.withHeader());
            // pickup_datetime & dropoff_datetime need to transform to timestamp
            rows = parser.getRecords();
            // TODO(hw): CSVParser will read the end empty line, be careful.
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
                    "index(key=id, ts=pickup_datetime))partitionnum=2" +
//                    "index(key=(vendor_id, passenger_count), ts=pickup_datetime),\n" +
//                    "index(key=passenger_count, ts=dropoff_datetime))\n" +
                    ";");
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

    // TODO(hw): 流程优先，所以先假设可以开MemTable个线程，分别负责。之后再考虑线程数有限的情况。
    private static void bulkLoadMulti(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        // MemTable.size() threads BulkLoadGenerator

        logger.info("query zk for table meta data");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("172.24.4.55:6181", retryPolicy);
        client.start();
        Nameserver.TableInfo testTable = null;
        Map<Integer, BulkLoadGenerator> generators = new HashMap<>();
        List<Thread> threads = new ArrayList<>();

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

            // When bulk loading, cannot AddIndex().
            //  And MemTable::table_index_ may be modified by AddIndex()/Delete...,
            //  so we should get table_index_'s info from MemTable, to know the real status.
            //  And the status can't be changed until bulk lood finished.
            for (Nameserver.TablePartition partition : testTable.getTablePartitionList()) {
                logger.info("tid-pid {}-{}, {}", testTable.getTid(), partition.getPid(), partition.getPartitionMetaList());
                Nameserver.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(Nameserver.PartitionMeta::getIsLeader).collect(onlyElement());

                //  http/h2 can't add attachment, cuz it use attachment to pass message. So we need to use brpc-java
                RpcClientOptions clientOption = new RpcClientOptions();
                clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
                clientOption.setWriteTimeoutMillis(1000);
                clientOption.setReadTimeoutMillis(50000);
                clientOption.setMaxTotalConnections(1000);
                clientOption.setMinIdleConnections(10);
                clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
                clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);

                // Must list://
                String serviceUrl = "list://" + leader.getEndpoint();
                RpcClient rpcClient = new RpcClient(serviceUrl, clientOption);
                TabletService tabletService = BrpcProxy.getProxy(rpcClient, TabletService.class);
                RpcContext.getContext().setLogId((long) partition.getPid());
                API.BulkLoadInfoRequest infoRequest = API.BulkLoadInfoRequest.newBuilder().setTid(testTable.getTid()).setPid(partition.getPid()).build();
                API.BulkLoadInfoResponse info = tabletService.getBulkLoadInfo(infoRequest);
                logger.debug("get bulk load info: {}", info);

                // generate & send requests by BulkLoadGenerator
                BulkLoadGenerator generator = new BulkLoadGenerator(testTable.getTid(), partition.getPid(), info, tabletService);
                generators.put(partition.getPid(), generator);
                threads.add(new Thread(generator));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        logger.info("create {} generators, start bulk load.", generators.size());
        threads.forEach(Thread::start);

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
        Preconditions.checkState(peekRecord.size() == schema.GetColumnCnt());

        List<String> stringCols = InsertImporter.getStringColumnsFromSchema(schema);
        // TODO(hw): check header?
//        logger.info("{}", peekRecord.getParser().getHeaderMap());
        for (CSVRecord record : rows) {
            // TODO(hw): can't use SQLInsertRow. GetRow() will get the wrong data. But we still need it to get dims & tsDims.
            SQLInsertRow row = router.getInsertRow(dbName, insertPlaceHolder);
            logger.info(record.toString());
            // fulfill insertRow
            int strLength = stringCols.stream().mapToInt(col -> record.get(col).length()).sum();
            row.Init(strLength);
            boolean rowIsValid = true;
            List<Object> rowValues = new ArrayList<>();
            for (int j = 0; j < schema.GetColumnCnt(); j++) {
                String v = record.get(schema.GetColumnName(j));
                DataType type = schema.GetColumnType(j);
                // TODO(hw): DataType doesn't have varchar, only string
                Object obj = buildTypedValues(v, type);
                Preconditions.checkNotNull(obj);
                rowValues.add(obj);

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

            // ColumnDesc.Type has varchar
            ByteBuffer buffer = RowBuilder.encode(rowValues.toArray(), testTable.getColumnDescList(), 1);
            Preconditions.checkState(testTable.getCompressType() == Type.CompressType.kNoCompress); // TODO(hw): snappy later
            buffer.rewind();
//            logger.info("byte buffer len {}", buffer.array().length); // checked

            // Feed row to the bulk load generators for each MemTable(pid, tid)
            DimMap dims = row.GetDimensions(); // dims of pid
            if (logger.isDebugEnabled()) {
                logger.debug("dims {}, tsDims {}", dims.entrySet().stream().map(entry -> entry.getKey().toString() + ": " +
                                entry.getValue().stream().map(pairStrInt ->
                                        "<" + pairStrInt.getFirst() + ", " + pairStrInt.getSecond() + ">")
                                        .collect(Collectors.joining(", ", "(", ")")))
                                .collect(Collectors.joining("], [", "[", "]")),
                        row.GetTs().toString());
            }
            for (Long pid : dims.keySet()) {
                try {
                    // nameserver pid is int
                    // TODO(hw): We still add rowData here, because the SQLInsertRow can't use GetRow() to get correct data.
                    //  Redundant row data, need improve.
                    generators.get(pid.intValue()).feed(row, buffer.array());
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

        // TODO(hw): get statistics from generators
        logger.info("BulkLoad finished.");
    }

    private static Object buildTypedValues(String v, DataType type) {
        if (DataType.kTypeBool.equals(type)) {
            return v.equals("true");
        } else if (DataType.kTypeInt16.equals(type)) {
            return Short.parseShort(v);
        } else if (DataType.kTypeInt32.equals(type)) {
            return (Integer.parseInt(v));
        } else if (DataType.kTypeInt64.equals(type)) {
            return Long.parseLong(v);
        } else if (DataType.kTypeFloat.equals(type)) {
            return Float.parseFloat(v);
        } else if (DataType.kTypeDouble.equals(type)) {
            return Double.parseDouble(v);
        } else if (DataType.kTypeString.equals(type)) {
            return v;
        } else if (DataType.kTypeDate.equals(type)) {
            return Date.valueOf(v);
        } else if (DataType.kTypeTimestamp.equals(type)) {
            // TODO(hw): no need to support data time. Converting here is only for simplify. May fix later.
            if (v.contains("-")) {
                Timestamp ts = Timestamp.valueOf(v);
                return ts.getTime(); // milliseconds
            }
            return Long.parseLong(v);
        }
        return null;
    }

    public static class SegmentIndexRegion {
        // TODO(hw): id ?
        final int tsCnt;
        final Map<Integer, Integer> tsIdxMap;
        public int ONE_IDX = 0;
        public int NO_IDX = 0;

        // TODO(hw): SegmentIndexMap treeMap, can do reverse iter? comparator1, Slice::compare, comparator2, TimeComparator
        Map<String, List<Map<Long, Integer>>> keyEntries = new TreeMap<>();

        public SegmentIndexRegion(int tsCnt, Map<Integer, Integer> tsIdxMap) {
            this.tsCnt = tsCnt;
            this.tsIdxMap = tsIdxMap; // possibly empty
        }

        private void Put(String key, int idxPos, Long time, Integer id) {
            List<Map<Long, Integer>> entryList = keyEntries.getOrDefault(key, new ArrayList<>());
            if (entryList.isEmpty()) {
                for (int i = 0; i < tsCnt; i++) {
                    entryList.add(new TreeMap<>());// TODO(hw): comparator?
                }
                keyEntries.put(key, entryList);
            }
            entryList.get(idxPos).put(time, id);
//            logger.info("after put keyEntries: {}", keyEntries.toString());
        }

        // TODO(hw): return val is only for debug
        public boolean Put(String key, List<API.TSDimension> tsDimensions, Integer dataBlockId) {
            if (tsDimensions.isEmpty()) {
                return false;
            }
            boolean put = false;
            if (tsCnt == 1) {
                if (tsDimensions.size() == 1) {
                    Put(key, this.NO_IDX, tsDimensions.get(0).getTs(), dataBlockId);
                    put = true;
                } else {
                    // tsCnt == 1 & has tsIdxMap, so tsIdxMap only has one element.
                    Preconditions.checkArgument(tsIdxMap.size() == 1);
                    Integer tsIdx = tsIdxMap.keySet().stream().collect(onlyElement());
                    API.TSDimension needPutIdx = tsDimensions.stream().filter(tsDimension -> tsDimension.getIdx() == tsIdx).collect(onlyElement());
                    Put(key, this.ONE_IDX, needPutIdx.getTs(), dataBlockId);
                    put = true;
                }
            } else {
                // tsCnt != 1, KeyEntry array for one key
                for (API.TSDimension tsDimension : tsDimensions) {
                    Integer pos = tsIdxMap.get(tsDimension.getIdx());
                    if (pos == null) {
                        continue;
                    }
                    Put(key, pos, tsDimension.getTs(), dataBlockId);
                    put = true;
                }
            }
            return put;
        }

        // serialize to `message Segment`
        public API.Segment toProtobuf() {
            API.Segment.Builder builder = API.Segment.newBuilder();
            keyEntries.forEach((key, keyEntry) -> {
                API.Segment.KeyEntries.Builder keyEntriesBuilder = builder.addKeyEntriesBuilder();
                keyEntriesBuilder.setKey(copyFromUtf8(key));
                keyEntry.forEach(timeEntries -> {
                    API.Segment.KeyEntries.KeyEntry.Builder keyEntryBuilder = keyEntriesBuilder.addKeyEntryBuilder();
                    timeEntries.forEach((time, blockId) -> {
                        API.Segment.KeyEntries.KeyEntry.TimeEntry.Builder timeEntryBuilder = keyEntryBuilder.addTimeEntryBuilder();
                        timeEntryBuilder.setTime(time).setBlockId(blockId);
                    });
                });
            });
            return builder.build();
        }
    }

    public static class BulkLoadRequest {
        // TODO(hw): private members below
        public List<List<SegmentIndexRegion>> segmentIndexMatrix;
        public ByteArrayOutputStream dataBlock = new ByteArrayOutputStream();
        public List<API.DataBlockInfo> dataBlockInfoList = new ArrayList<>();

        public BulkLoadRequest(API.BulkLoadInfoResponse bulkLoadInfo) {
            segmentIndexMatrix = new ArrayList<>();

            bulkLoadInfo.getInnerSegmentsList().forEach(
                    innerSegments -> {
                        List<SegmentIndexRegion> segments = new ArrayList<>();
                        innerSegments.getSegmentList().forEach(
                                segmentInfo -> {
                                    // ts_idx_map array to map, proto2 doesn't support map.
                                    Map<Integer, Integer> tsIdxMap = segmentInfo.getTsIdxMapList().stream().collect(Collectors.toMap(
                                            API.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getKey,
                                            API.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getValue)); // can't tolerate dup key
                                    int tsCnt = segmentInfo.getTsCnt();
                                    segments.add(new SegmentIndexRegion(tsCnt, tsIdxMap));
                                }
                        );
                        this.segmentIndexMatrix.add(segments);
                    }
            );
        }

        public API.BulkLoadRequest toProtobuf(int tid, int pid) {
            API.BulkLoadRequest.Builder requestBuilder = API.BulkLoadRequest.newBuilder();
            requestBuilder.setTid(tid).setPid(pid);

            // segmentDataMaps -> BulkLoadIndex
            segmentIndexMatrix.forEach(segmentDataMap -> {
                API.BulkLoadIndex.Builder bulkLoadIndexBuilder = requestBuilder.addIndexRegionBuilder();
//                // TODO(hw):
//                bulkLoadIndexBuilder.setInnerIndexId();
                segmentDataMap.forEach(segment -> {
                    bulkLoadIndexBuilder.addSegment(segment.toProtobuf());
                });
            });
            // DataBlockInfo
            requestBuilder.addAllBlockInfo(dataBlockInfoList);

            return requestBuilder.build();
        }

        public byte[] getDataRegion() {
            // TODO(hw): hard copy, can be avoided? utf8?
            return dataBlock.toByteArray();
        }

    }

    // TODO(hw): murmurhash2 java version, check correctness
    // signed int
    public static int hash(final byte[] data, int length, int seed) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= (data[length & ~3] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}

class BulkLoadGenerator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoadGenerator.class);
    private final int tid;
    private final int pid;
    private final BlockingQueue<Pair<SQLInsertRow, byte[]>> queue;
    private final long pollTimeout;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean internalErrorOcc = new AtomicBoolean(false);
    private final API.BulkLoadInfoResponse indexInfo; // TODO(hw): not a good name
    private final Main.BulkLoadRequest bulkLoadRequest;
    private final TabletService service;

    public BulkLoadGenerator(int tid, int pid, API.BulkLoadInfoResponse indexInfo, TabletService service) {
        this.tid = tid;
        this.pid = pid;
        this.queue = new ArrayBlockingQueue<>(1000);
        this.pollTimeout = 100;
        this.indexInfo = indexInfo;
        this.bulkLoadRequest = new Main.BulkLoadRequest(indexInfo); // built from BulkLoadInfoResponse
        this.service = service;
    }

    @Override
    public void run() {
        logger.info("Thread {} for MemTable(tid-pid {}-{})", Thread.currentThread().getId(), tid, pid);
        try {
            // exit statement: shutdown and no element in queue, or internal exit
            while (!shutdown.get() || !this.queue.isEmpty()) {
                Pair<SQLInsertRow, byte[]> pair = this.queue.poll(this.pollTimeout, TimeUnit.MILLISECONDS);
                if (pair == null) {
                    // poll timeout, queue is still empty
                    continue;
                }
                SQLInsertRow row = pair.getLeft();
                byte[] rowData = pair.getRight();
                if (row == null || rowData.length == 0) {
                    logger.warn("invalid row or data");
                    continue;
                }

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
                int dataBlockId = bulkLoadRequest.dataBlockInfoList.size();
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
                        long segIdx = 0;
                        if (indexInfo.getSegCnt() > 1) {
                            // hash get signed int, we treat is as unsigned
                            segIdx = Integer.toUnsignedLong(Main.hash(key.getBytes(), key.length(), 0xe17a1465)) % indexInfo.getSegCnt();
                        }
                        // TODO(hw): segment[k][segIdx]->Put. only in-memory first.
                        Main.SegmentIndexRegion segment = bulkLoadRequest.segmentIndexMatrix.get(idx).get((int) segIdx);

                        // void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row)
                        boolean put = segment.Put(key, tsDimsWrap, dataBlockId);
                        if (!put) {
                            logger.warn("segment.Put no put");
                        }
                    }
                }

                // If success, add data & info
                ByteArrayOutputStream dataBuilder = bulkLoadRequest.dataBlock;
                logger.debug("bulk load one row data size {}", rowData.length);
                int head = dataBuilder.size();
                dataBuilder.write(rowData);
                dataBlockInfoBuilder.setRefCnt(realRefCnt.get()).setOffset(head).setLength(rowData.length);
                bulkLoadRequest.dataBlockInfoList.add(dataBlockInfoBuilder.build());
                // TODO(hw): multi-threading insert into one MemTable dataHolder: needs lock?

            }

            // TODO(hw): force shutdown - don't send rpc

            // TODO(hw): request -> pb
            logger.info("bulkLoadRequest brief info: total rows {}, data total size {}", bulkLoadRequest.dataBlockInfoList.size(), bulkLoadRequest.dataBlock.size());
            API.BulkLoadRequest request = bulkLoadRequest.toProtobuf(tid, pid);
            if (logger.isDebugEnabled()) {
                logger.debug("bulk load request {}", request);
            }
            // TODO(hw): send rpc
            RpcContext.getContext().setRequestBinaryAttachment(bulkLoadRequest.getDataRegion());
            API.GeneralResponse response = service.bulkLoad(request);
            if (logger.isDebugEnabled()) {
                logger.debug("bulk load resp: {}", response);
            }
        } catch (Exception e) {
            // TODO(hw): IOException - byte array write
            e.printStackTrace();
            internalErrorOcc.set(true);
        }
    }

    public void feed(SQLInsertRow row, byte[] rowData) throws InterruptedException {
        this.queue.put(Pair.of(row, rowData)); // blocking put
    }

    public void shutDown() {
        this.shutdown.set(true);
    }

    public boolean hasInternalError() {
        return internalErrorOcc.get();
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
                logger.debug("insert one row data size {}", row.GetRow().length());
                boolean ok = router.executeInsert(dbName, insertPlaceHolder, row);
                // TODO(hw): retry
                if (!ok) {
                    logger.error("insert one row failed, {}", record);
                }
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