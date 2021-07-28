package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.SQLInsertRow;
import com._4paradigm.openmldb.Schema;
import com._4paradigm.openmldb.api.Tablet;
import com._4paradigm.openmldb.common.Common;
import com._4paradigm.openmldb.ns.NS;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.baidu.brpc.RpcContext;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.loadbalance.LoadBalanceStrategy;
import com.baidu.brpc.protocol.Options;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
//            Reader in = new FileReader("/home/huangwei/NYCTaxiDataset/train.csv"); //big 1.4G // 192M
            Reader in = new FileReader("/home/huangwei/NYCTaxiDataset/train.csv.small"); // debug
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
        option.setZkCluster("172.24.4.55:4181");
        option.setZkPath("/onebox");

        int X = 8; // put_concurrency_limit default is 8
        int rpcDataSizeLimit = 2 * 1000 * 1000; // 2MB

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
                    "index(key=id, ts=pickup_datetime)) OPTIONS (partitionnum=" + X +
//                    "index(key=(vendor_id, passenger_count), ts=pickup_datetime),\n" +
//                    "index(key=passenger_count, ts=dropoff_datetime))\n" +
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

        logger.info("set thread num {}", X);

        logger.info("rows {}, peek {}", rows.size(), rows.isEmpty() ? "" : rows.get(0).toString());
        long startTime = System.currentTimeMillis();

//        insertImport(X, rows, router, dbName, tableName);

        bulkLoadMulti(X, rows, router, dbName, tableName, rpcDataSizeLimit);

        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;

        router.close();

        logger.info("End. Total time: {} ms", totalTime);
    }

    private static void insertImport(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        int quotient = rows.size() / X;
        int remainder = rows.size() % X;
        List<Pair<Integer, Integer>> ranges = new ArrayList<>();
        int start = 0;
        // [left, right]
        for (int i = 0; i < remainder; ++i) {
            int rangeEnd = Math.min(start + quotient + 1, rows.size());
            ranges.add(Pair.of(start, rangeEnd));
            start = rangeEnd;
        }
        for (int i = remainder; i < X; ++i) {
            int rangeEnd = Math.min(start + quotient, rows.size());
            ranges.add(Pair.of(start, rangeEnd));
            start = rangeEnd;
        }

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
    private static void bulkLoadMulti(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName, int rpcDataSizeLimit) {
        // MemTable.size() threads BulkLoadGenerator
        logger.info("get schema by sdk");
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

        // TODO(hw): check header?
        //  logger.info("{}", peekRecord.getParser().getHeaderMap());

        logger.info("query zk for table meta data");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("172.24.4.55:4181", retryPolicy);
        client.start();
        NS.TableInfo testTable = null;
        Map<Integer, BulkLoadGenerator> generators = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        List<RpcClient> rpcClients = new ArrayList<>();

        try {
            String tableInfoPath = "/onebox/table/db_table_data";
            List<String> tables = client.getChildren().forPath(tableInfoPath);
            Preconditions.checkNotNull(tables, "zk path {} get child failed.", tableInfoPath);

            for (String tableId : tables) {
                byte[] tableInfo = client.getData().forPath(tableInfoPath + "/" + tableId);
                NS.TableInfo info = NS.TableInfo.parseFrom(tableInfo);
                logger.debug(info.toString());
                if (info.getName().equals(tableName)) {
                    testTable = info;
                }
            }
            Preconditions.checkNotNull(testTable, "no table info of table {}", tableName);

            // When bulk loading, cannot AddIndex().
            //  And MemTable::table_index_ may be modified by AddIndex()/Delete...,
            //  so we should get table_index_'s info from MemTable, to know the real status.
            //  And the status can't be changed until bulk lood finished.
            for (NS.TablePartition partition : testTable.getTablePartitionList()) {
                logger.info("tid-pid {}-{}, {}", testTable.getTid(), partition.getPid(), partition.getPartitionMetaList());
                NS.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(NS.PartitionMeta::getIsLeader).collect(onlyElement());

                //  http/h2 can't add attachment, cuz it use attachment to pass message. So we need to use brpc-java
                RpcClientOptions clientOption = new RpcClientOptions();
                clientOption.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
                clientOption.setWriteTimeoutMillis(1000);
                clientOption.setReadTimeoutMillis(50000);
                clientOption.setMaxTotalConnections(1000);
                clientOption.setMinIdleConnections(10);
                clientOption.setLoadBalanceType(LoadBalanceStrategy.LOAD_BALANCE_FAIR);
                clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
                clientOption.setGlobalThreadPoolSharing(true);

                // Must list://
                String serviceUrl = "list://" + leader.getEndpoint();
                RpcClient rpcClient = new RpcClient(serviceUrl, clientOption);
                TabletService tabletService = BrpcProxy.getProxy(rpcClient, TabletService.class);
                RpcContext.getContext().setLogId((long) partition.getPid());
                Tablet.BulkLoadInfoRequest infoRequest = Tablet.BulkLoadInfoRequest.newBuilder().setTid(testTable.getTid()).setPid(partition.getPid()).build();
                Tablet.BulkLoadInfoResponse bulkLoadInfo = tabletService.getBulkLoadInfo(infoRequest);
                logger.debug("get bulk load info: {}", bulkLoadInfo);

                // generate & send requests by BulkLoadGenerator
                // TODO(hw): schema could get from NS.TableInfo?
                BulkLoadGenerator generator = new BulkLoadGenerator(testTable.getTid(), partition.getPid(), testTable, bulkLoadInfo, tabletService, rpcDataSizeLimit);
                generators.put(partition.getPid(), generator);
                threads.add(new Thread(generator));
                // To close all clients
                rpcClients.add(rpcClient);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        logger.info("create {} generators, start bulk load.", generators.size());
        threads.forEach(Thread::start);

        // testTable(TableInfo) to index map
        Preconditions.checkState(testTable.getColumnKeyCount() > 0); // TODO(hw): no col key is available?
        Map<String, Integer> nameToIdx = new HashMap<>();
        for (int i = 0; i < testTable.getColumnDescCount(); i++) {
            nameToIdx.put(testTable.getColumnDesc(i).getName(), i);
        }
        Map<Integer, List<Integer>> keyIndexMap = new HashMap<>();
        Set<Integer> tsIdxSet = new HashSet<>();
        for (int i = 0; i < testTable.getColumnKeyCount(); i++) {
            Common.ColumnKey key = testTable.getColumnKey(i);
            for (String colName : key.getColNameList()) {
                List<Integer> keyCols = keyIndexMap.getOrDefault(i, new ArrayList<>());
                keyCols.add(nameToIdx.get(colName));
                keyIndexMap.put(i, keyCols);
            }
            if (key.hasTsName()) {
                tsIdxSet.add(nameToIdx.get(key.getTsName()));
            }
        }

        for (CSVRecord record : rows) {
            Map<Integer, List<Pair<String, Integer>>> dims = buildDimensions(record, keyIndexMap, testTable.getPartitionNum());
            if (logger.isDebugEnabled()) {
                logger.debug(record.toString());
                logger.debug(dims.entrySet().stream().map(entry -> entry.getKey().toString() + ": " +
                        entry.getValue().stream().map(pair ->
                                "<" + pair.getKey() + ", " + pair.getValue() + ">")
                                .collect(Collectors.joining(", ", "(", ")")))
                        .collect(Collectors.joining("], [", "[", "]")));
            }

            // Feed row to the bulk load generators for each MemTable(pid, tid)
            for (Integer pid : dims.keySet()) {
                try {
                    // NS pid is int
                    // TODO(hw): no need to calc dims twice
                    generators.get(pid).feed(new BulkLoadGenerator.FeedItem(dims, tsIdxSet, record));
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

        // rpc client release
        rpcClients.forEach(RpcClient::stop);
    }

    // ref SQLInsertRow::GetDimensions()
    // TODO(hw): integer or long?
    private static Map<Integer, List<Pair<String, Integer>>> buildDimensions(CSVRecord record, Map<Integer, List<Integer>> keyIndexMap, int pidNum) {
        Map<Integer, List<Pair<String, Integer>>> dims = new HashMap<>();
        int pid = 0;
        for (Map.Entry<Integer, List<Integer>> entry : keyIndexMap.entrySet()) {
            Integer index = entry.getKey();
            List<Integer> keyCols = entry.getValue();
            String combinedKey = keyCols.stream().map(record::get).collect(Collectors.joining("|"));
            if (pidNum > 0) {
                pid = (int) Math.abs(MurmurHash.hash64(combinedKey) % pidNum);
            }
            List<Pair<String, Integer>> dim = dims.getOrDefault(pid, new ArrayList<>());
            dim.add(Pair.of(combinedKey, index));
            dims.put(pid, dim);
        }
        return dims;
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

