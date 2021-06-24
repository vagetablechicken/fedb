package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.api.Tablet;
import com._4paradigm.openmldb.common.Common;
import com._4paradigm.openmldb.ns.NS;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;

@CommandLine.Command(name = "data importer", mixinStandardHelpOptions = true, description = "insert/bulk load data(csv) to openmldb")
public class DataImporter {
    private static final Logger logger = LoggerFactory.getLogger(DataImporter.class);

    enum Mode {
        Insert,
        BulkLoad
    }

    @CommandLine.Option(names = "--importer_mode", defaultValue = "BulkLoad", description = "mode: ${COMPLETION-CANDIDATES}. Case insensitive.")
    Mode mode;

    @CommandLine.Option(names = "--files", split = ",", required = true)
    private List<String> files;

    @CommandLine.Option(names = {"--zk_cluster", "-z"}, description = "zookeeper cluster address of openmldb", required = true)
    private String zkCluster;
    @CommandLine.Option(names = "--zk_root_path", description = "", required = true)
    private String zkRootPath;
    @CommandLine.Option(names = "--db", required = true)
    private String dbName;
    @CommandLine.Option(names = "--table", required = true)
    private String tableName;
    @CommandLine.Option(names = "--create_ddl")
    private String createDDL;

    @CommandLine.Option(names = {"-f", "--force_recreate_table"})
    private boolean forceRecreateTable;

    @CommandLine.Option(names = "--rpc_size_limit")
    private int rpcDataSizeLimit = 32 * 1024 * 1024; // 32MB

    LocalFilesReader reader = null;
    SqlExecutor router = null;

    // src: file paths
    // read dir or *.xx?
    // how about SQL "LOAD DATA INFILE"? May need hdfs sasl config
    public boolean setUpSrcReader() {
        logger.info("files: {}", files);
        if (files.isEmpty()) {
            logger.info("config 'files' is empty");
            return false;
        }

        try {
            // TODO(hw): check csv header?
            reader = new LocalFilesReader(files);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean setUpSDK() {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkRootPath);
        try {
            router = new SqlClusterExecutor(option);
            return true;
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        return false;
    }

    public boolean checkTable() {
        Preconditions.checkNotNull(router);
        if (forceRecreateTable) {
            router.executeDDL(dbName, "drop table " + tableName + ";");
        } else if (router.executeDDL(dbName, "desc table " + tableName + ";")) {
            return true;
        }

        // try create table
        if (createDDL == null) {
            return false;
        }
        // if db is not existed, create it
        router.createDB(dbName);
        // create table
        return router.executeDDL(dbName, createDDL);
    }

    public void Load() {
        if (mode == Mode.Insert) {
            logger.error("insert mode needs refactor, unsupported now");
            return;
        }

        NS.TableInfo tableMetaData = getTableMetaData();
        if (tableMetaData == null) {
            logger.error("table {} meta data is not found", tableName);
            return;
        }

        logger.debug(tableMetaData.toString());
        // TODO(hw): multi-threading insert into one MemTable? or threads num is less than MemTable size?
        logger.info("create generators for each table partition(MemTable)");
        Map<Integer, BulkLoadGenerator> generators = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        //  http/h2 can't add attachment, cuz it use attachment to pass message. So we need to use brpc-java RpcClient
        List<RpcClient> rpcClients = new ArrayList<>();
        try {
            // When bulk loading, cannot AddIndex().
            //  And MemTable::table_index_ may be modified by AddIndex()/Delete...,
            //  so we should get table_index_'s info from MemTable, to know the real status.
            //  And the status can't be changed until bulk lood finished.
            for (NS.TablePartition partition : tableMetaData.getTablePartitionList()) {
                logger.debug("tid-pid {}-{}, {}", tableMetaData.getTid(), partition.getPid(), partition.getPartitionMetaList());
                NS.PartitionMeta leader = partition.getPartitionMetaList().stream().filter(NS.PartitionMeta::getIsLeader).collect(onlyElement());

                RpcClientOptions clientOption = getRpcClientOptions();

                // Must list://
                String serviceUrl = "list://" + leader.getEndpoint();
                RpcClient rpcClient = new RpcClient(serviceUrl, clientOption);
                TabletService tabletService = BrpcProxy.getProxy(rpcClient, TabletService.class);
                Tablet.BulkLoadInfoRequest infoRequest = Tablet.BulkLoadInfoRequest.newBuilder().setTid(tableMetaData.getTid()).setPid(partition.getPid()).build();
                Tablet.BulkLoadInfoResponse bulkLoadInfo = tabletService.getBulkLoadInfo(infoRequest);
                logger.debug("get bulk load info of {} : {}", leader.getEndpoint(), bulkLoadInfo);

                // generate & send requests by BulkLoadGenerator
                // we need schema to parsing raw data in generator, got from NS.TableInfo
                BulkLoadGenerator generator = new BulkLoadGenerator(tableMetaData.getTid(), partition.getPid(), tableMetaData, bulkLoadInfo, tabletService, rpcDataSizeLimit);
                generators.put(partition.getPid(), generator);
                threads.add(new Thread(generator));

                // To close all clients
                rpcClients.add(rpcClient);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        logger.info("create {} generators, start bulk load...", generators.size());
        threads.forEach(Thread::start);

        // tableMetaData(TableInfo) to index map, needed by buildDimensions.
        // We build dimensions here, to distribute data to MemTables which should load the data.
        Preconditions.checkState(tableMetaData.getColumnKeyCount() > 0); // TODO(hw): no col key is available?

        Map<Integer, List<Integer>> keyIndexMap = new HashMap<>();
        Set<Integer> tsIdxSet = new HashSet<>();
        parseIndexMapAndTsSet(tableMetaData, keyIndexMap, tsIdxSet);
        try {
            CSVRecord record;
            while ((record = reader.next()) != null) {
                Map<Integer, List<Pair<String, Integer>>> dims = buildDimensions(record, keyIndexMap, tableMetaData.getPartitionNum());
                if (logger.isDebugEnabled()) {
                    logger.debug(record.toString());
                    logger.debug(dims.entrySet().stream().map(entry -> entry.getKey().toString() + ": " +
                            entry.getValue().stream().map(pair ->
                                    "<" + pair.getKey() + ", " + pair.getValue() + ">")
                                    .collect(Collectors.joining(", ", "(", ")")))
                            .collect(Collectors.joining("], [", "[", "]")));
                }

                // distribute the row to the bulk load generators for each MemTable(tid, pid)
                for (Integer pid : dims.keySet()) {
                    // Note: NS pid is int
                    // no need to calc dims twice, pass it to BulkLoadGenerator
                    generators.get(pid).feed(new BulkLoadGenerator.FeedItem(dims, tsIdxSet, record));
                }
            }
        } catch (Exception e) {
            logger.error("feeding failed, {}", e.getMessage());
        }

        generators.forEach((integer, bulkLoadGenerator) -> bulkLoadGenerator.shutdownGracefully());
        logger.info("shutdown gracefully, waiting threads...");
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (generators.values().stream().anyMatch(BulkLoadGenerator::hasInternalError)) {
            logger.error("BulkLoad has failed.");
        }

        // TODO(hw): get statistics from generators
        logger.info("BulkLoad finished.");

        // rpc client release
        rpcClients.forEach(RpcClient::stop);
    }

    private RpcClientOptions getRpcClientOptions() {
        RpcClientOptions clientOption = new RpcClientOptions();
        // clientOption.setWriteTimeoutMillis(1000);
        clientOption.setReadTimeoutMillis(50000); // index rpc may take time, cuz need to do bulk load
        // clientOption.setMinIdleConnections(10);
        // clientOption.setCompressType(Options.CompressType.COMPRESS_TYPE_NONE);
        clientOption.setGlobalThreadPoolSharing(true);
        return clientOption;
    }

    private NS.TableInfo getTableMetaData() {
        try {
            logger.info("query zk for table {} meta data", tableName);
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(zkCluster, retryPolicy);
            client.start();
            String tableInfoPath = zkRootPath + "/table/db_table_data";
            List<String> tables = client.getChildren().forPath(tableInfoPath);
            Preconditions.checkNotNull(tables, "zk path {} get child failed.", tableInfoPath);

            for (String tableId : tables) {
                byte[] tableInfo = client.getData().forPath(tableInfoPath + "/" + tableId);
                NS.TableInfo info = NS.TableInfo.parseFrom(tableInfo);
                if (info.getName().equals(tableName)) {
                    return info;
                }
            }
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
        return null;
    }

    public void close() {
        router.close();
    }

    // TODO(hw): load from properties.file, cmd可以覆盖配置，这样好一点？cmd不适合太长
    public static void main(String[] args) {
        DataImporter dataImporter = new DataImporter();
        CommandLine cmd = new CommandLine(dataImporter).setCaseInsensitiveEnumValuesAllowed(true);
        // TODO(hw): file path
        File defaultsFile = new File("src/main/resources/importer.properties");
        cmd.setDefaultValueProvider(new CommandLine.PropertiesDefaultProvider(defaultsFile));
        cmd.parseArgs(args);

        logger.info("Start...");

        if (!dataImporter.setUpSrcReader()) {
            logger.error("set up src reader failed");
            return;
        }
        if (!dataImporter.setUpSDK()) {
            logger.error("set up sdk failed");
            return;
        }
        if (!dataImporter.checkTable()) {
            logger.error("check table failed, try to create, failed too(no create ddl sql or create table failed)");
            return;
        }

        logger.info("do load");
//        int X = 8; // put_concurrency_limit default is 8

        // TODO(hw): check arg rpcDataSizeLimit >= minLimitSize

        long startTime = System.currentTimeMillis();

        dataImporter.Load();

        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;

        dataImporter.close();
        logger.info("End. Total time: {} ms", totalTime);
    }

    // TODO(hw): insert import mode refactor. limited retry, report the real-time status of progress.
    // can't find a good import tool framework, may ref mysqlimport
    private static void insertImportByRange(int X, List<CSVRecord> rows, SqlExecutor router, String dbName, String tableName) {
        int quotient = rows.size() / X;
        int remainder = rows.size() % X;
        // range is [left, right)
        List<Pair<Integer, Integer>> ranges = new ArrayList<>();
        int start = 0;
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

        List<Thread> threads = new ArrayList<>();
        for (Pair<Integer, Integer> range : ranges) {
            threads.add(new Thread(new InsertImporter(router, dbName, tableName, rows, range)));
        }

        threads.forEach(Thread::start);

        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        });

    }

    private static void parseIndexMapAndTsSet(NS.TableInfo table, Map<Integer, List<Integer>> keyIndexMap, Set<Integer> tsIdxSet) {
        Map<String, Integer> nameToIdx = new HashMap<>();
        for (int i = 0; i < table.getColumnDescCount(); i++) {
            nameToIdx.put(table.getColumnDesc(i).getName(), i);
        }
        for (int i = 0; i < table.getColumnKeyCount(); i++) {
            Common.ColumnKey key = table.getColumnKey(i);
            for (String colName : key.getColNameList()) {
                List<Integer> keyCols = keyIndexMap.getOrDefault(i, new ArrayList<>());
                keyCols.add(nameToIdx.get(colName));
                keyIndexMap.put(i, keyCols);
            }
            if (key.hasTsName()) {
                tsIdxSet.add(nameToIdx.get(key.getTsName()));
            }
        }
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
}

