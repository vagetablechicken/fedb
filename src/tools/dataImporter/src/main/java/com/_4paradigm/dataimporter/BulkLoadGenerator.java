package com._4paradigm.dataimporter;

import com._4paradigm.fedb.api.API;
import com._4paradigm.fedb.common.Common;
import com._4paradigm.fedb.nameserver.Nameserver;
import com._4paradigm.fedb.type.Type;
import com.baidu.brpc.RpcContext;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkLoadGenerator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoadGenerator.class);

    public static class FeedItem {
        public final Map<Integer, List<Pair<String, Integer>>> dims;
        public final List<Long> tsDims; // TODO(hw): how to use uint64? BigInteger is low-effective
        public final Map<String, String> valueMap;

        public FeedItem(Map<Integer, List<Pair<String, Integer>>> dims, Set<Integer> tsSet, CSVRecord record) {
            this.dims = dims;
            this.valueMap = record.toMap();
            // TODO(hw): can't support no header csv now
            Preconditions.checkNotNull(valueMap);
            // TODO(hw): build tsDims here!! copy then improve
            tsDims = new ArrayList<>();
            for (Integer tsPos : tsSet) {
                // only kTimeStamp type
                tsDims.add((Long) buildTypedValues(record.get(tsPos), Type.DataType.kTimestamp));
            }
        }
    }

    private final int tid;
    private final int pid;
    private final BlockingQueue<FeedItem> queue;
    private final long pollTimeout;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean internalErrorOcc = new AtomicBoolean(false);
    private final Nameserver.TableInfo tableInfo;
    private final API.BulkLoadInfoResponse indexInfoFromTablet; // TODO(hw): not a good name
    private final BulkLoadRequest bulkLoadRequest;
    private final TabletService service;

    public BulkLoadGenerator(int tid, int pid, Nameserver.TableInfo tableInfo, API.BulkLoadInfoResponse indexInfo, TabletService service) {
        this.tid = tid;
        this.pid = pid;
        this.queue = new ArrayBlockingQueue<>(1000);
        this.pollTimeout = 100;
        this.tableInfo = tableInfo;
        this.indexInfoFromTablet = indexInfo;
        this.bulkLoadRequest = new BulkLoadRequest(indexInfoFromTablet); // built from BulkLoadInfoResponse
        this.service = service;
    }

    @Override
    public void run() {
        logger.info("Thread {} for MemTable(tid-pid {}-{})", Thread.currentThread().getId(), tid, pid);

        try {
            // exit statement: shutdown and no element in queue, or internal exit
            long startTime = System.currentTimeMillis();
            long realGenTime = 0;
            while (!shutdown.get() || !this.queue.isEmpty()) {
                FeedItem item = this.queue.poll(this.pollTimeout, TimeUnit.MILLISECONDS);
                if (item == null) {
                    // poll timeout, queue is still empty
                    continue;
                }
                long realStartTime = System.currentTimeMillis();

                List<Object> rowValues = new ArrayList<>();
                for (int j = 0; j < tableInfo.getColumnDescCount(); j++) {
                    Common.ColumnDesc desc = tableInfo.getColumnDesc(j);
                    String v = item.valueMap.get(desc.getName());
                    Type.DataType type = desc.getDataType();
                    // TODO(hw): DataType doesn't have varchar, only string
                    Object obj = buildTypedValues(v, type);
                    Preconditions.checkNotNull(obj);
                    rowValues.add(obj);
                }

                // getColumnDescList -> ColumnDesc.Type has varchar
                ByteBuffer dataBuffer = RowBuilder.encode(rowValues.toArray(), tableInfo.getColumnDescList(), 1);
                Preconditions.checkState(tableInfo.getCompressType() == Type.CompressType.kNoCompress); // TODO(hw): snappy later
                dataBuffer.rewind();

                // TODO(hw): get dims by pid, needs swig support
                //  拿insertRow的dims等信息，需要改sdk swig，但java复制步骤代码量不小，还是改sdk更容易。
                List<Pair<String, Integer>> dimensions = item.dims.get(this.pid);

                // tsDimensions[idx] has 0 or 1 ts, we convert it to API.TSDimensions for simplicity
                List<API.TSDimension> tsDimensions = new ArrayList<>();
                for (int i = 0; i < item.tsDims.size(); i++) {
                    tsDimensions.add(API.TSDimension.newBuilder().setIdx(i).setTs(item.tsDims.get(i)).build());
                }
                // If no ts, use current time
                long time = System.currentTimeMillis();

                Map<Integer, String> innerIndexKeyMap = new HashMap<>();
                // PairStrInt: str-key, int-idx. == message Dimension
                for (Pair<String, Integer> dim : dimensions) {
                    String key = dim.getKey();
                    long idx = dim.getValue();
                    // TODO(hw): idx is uint32, but info size is int
                    Preconditions.checkElementIndex((int) idx, indexInfoFromTablet.getInnerIndexCount());
                    innerIndexKeyMap.put(indexInfoFromTablet.getInnerIndexPos((int) idx), key);
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
                    API.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfoFromTablet.getInnerIndex(k);
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
                    API.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfoFromTablet.getInnerIndex(idx);
                    boolean needPut = innerIndex.getIndexDefList().stream().anyMatch(API.BulkLoadInfoResponse.InnerIndexSt.IndexDef::getIsReady);
                    if (needPut) {
                        long segIdx = 0;
                        if (indexInfoFromTablet.getSegCnt() > 1) {
                            // hash get signed int, we treat is as unsigned
                            segIdx = Integer.toUnsignedLong(Main.hash(key.getBytes(), key.length(), 0xe17a1465)) % indexInfoFromTablet.getSegCnt();
                        }
                        // TODO(hw): segment[k][segIdx]->Put. only in-memory first.
                        BulkLoadRequest.SegmentIndexRegion segment = bulkLoadRequest.segmentIndexMatrix.get(idx).get((int) segIdx);

                        // void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row)
                        boolean put = segment.Put(key, tsDimsWrap, dataBlockId);
                        if (!put) {
                            logger.warn("segment.Put no put");
                        }
                    }
                }

                // If success, add data & info
                ByteArrayOutputStream dataBuilder = bulkLoadRequest.dataBlock;
                int head = dataBuilder.size();
                dataBuilder.write(dataBuffer.array());
                int length = dataBuilder.size() - head;
                logger.debug("bulk load one row data size {}", length);
                dataBlockInfoBuilder.setRefCnt(realRefCnt.get()).setOffset(head).setLength(length);
                bulkLoadRequest.dataBlockInfoList.add(dataBlockInfoBuilder.build());
                // TODO(hw): multi-threading insert into one MemTable dataHolder: needs lock?
                long realEndTime = System.currentTimeMillis();
                realGenTime += (realEndTime - realStartTime);
            }

            long generateTime = System.currentTimeMillis();
            logger.info("Thread {} for MemTable(tid-pid {}-{}), generate cost {} ms, real cost {} ms",
                    Thread.currentThread().getId(), tid, pid, generateTime - startTime, realGenTime);
            // TODO(hw): force shutdown - don't send rpc

            // TODO(hw): request -> pb
            logger.info("bulkLoadRequest brief info: total rows {}, data total size {}",
                    bulkLoadRequest.dataBlockInfoList.size(), bulkLoadRequest.dataBlock.size());
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
            long endTime = System.currentTimeMillis();
            logger.info("rpc cost {} ms", endTime - generateTime);
        } catch (Exception e) {
            // TODO(hw): IOException - byte array write
            e.printStackTrace();
            internalErrorOcc.set(true);
        }
    }

    public void feed(FeedItem item) throws InterruptedException {
        this.queue.put(item); // blocking put
    }

    public void shutDown() {
        this.shutdown.set(true);
    }

    public boolean hasInternalError() {
        return internalErrorOcc.get();
    }

    private static Object buildTypedValues(String v, Type.DataType type) {
        switch (type) {
            case kBool:
                return v.equals("true");
            case kSmallInt:
                return Short.parseShort(v);
            case kInt:
                return (Integer.parseInt(v));
            case kBigInt:
                return Long.parseLong(v);
            case kFloat:
                return Float.parseFloat(v);
            case kDouble:
                return Double.parseDouble(v);
            case kVarchar:
            case kString:
                return v;
            case kDate:
                return Date.valueOf(v);
            case kTimestamp:
                // TODO(hw): no need to support data time. Converting here is only for simplify. Should be deleted later.
                if (v.contains("-")) {
                    Timestamp ts = Timestamp.valueOf(v);
                    return ts.getTime(); // milliseconds
                }
                return Long.parseLong(v);
        }
        return null;
    }
}
