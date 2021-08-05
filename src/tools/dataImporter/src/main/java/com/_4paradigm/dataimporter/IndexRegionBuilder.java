/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.api.Tablet;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.protobuf.ByteString.copyFromUtf8;

// IndexRegionBuilder builds requests of index region(split by size).
// Must build and send index requests after all data requests have been sent.
public class IndexRegionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(IndexRegionBuilder.class);

    private final int tid;
    private final int pid;
    private final List<List<SegmentIndexRegion>> segmentIndexMatrix; // TODO(hw): to map?

    private int partId = 0; // TODO(hw): start after data region part id

    public IndexRegionBuilder(int tid, int pid, Tablet.BulkLoadInfoResponse bulkLoadInfo) {
        this.tid = tid;
        this.pid = pid;
        segmentIndexMatrix = new ArrayList<>();

        bulkLoadInfo.getInnerSegmentsList().forEach(
                innerSegments -> {
                    List<SegmentIndexRegion> segments = new ArrayList<>();
                    innerSegments.getSegmentList().forEach(
                            segmentInfo -> {
                                // ts_idx_map array to map, proto2 doesn't support map.
                                Map<Integer, Integer> tsIdxMap = segmentInfo.getTsIdxMapList().stream().collect(Collectors.toMap(
                                        Tablet.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getKey,
                                        Tablet.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getValue)); // can't tolerate dup key
                                int tsCnt = segmentInfo.getTsCnt();
                                segments.add(new SegmentIndexRegion(tsCnt, tsIdxMap));
                            }
                    );
                    this.segmentIndexMatrix.add(segments);
                }
        );
    }



    public Tablet.BulkLoadRequest buildIndexRequest() {
        Tablet.BulkLoadRequest.Builder requestBuilder = Tablet.BulkLoadRequest.newBuilder();
        setRequest(requestBuilder);
        // TODO(hw): hard copy, can be avoided? utf8?
        return requestBuilder.build();
    }

    private void setRequest(Tablet.BulkLoadRequest.Builder requestBuilder) {
        // segmentDataMaps -> BulkLoadIndex
        int indexCount = indexCount();
        if (indexCount > 0) {
            logger.info("index count {}", indexCount);
            segmentIndexMatrix.forEach(segmentDataMap -> {
                Tablet.BulkLoadIndex.Builder bulkLoadIndexBuilder = requestBuilder.addIndexRegionBuilder();
                // TODO(hw): if we split index rpc, matrix should be a map？
//                bulkLoadIndexBuilder.setInnerIndexId();
                segmentDataMap.forEach(segment -> bulkLoadIndexBuilder.addSegment(segment.toProtobuf()));
            });
        }

        requestBuilder.setTid(tid).setPid(pid).setDataPartId(partId);
        partId++;
    }

    public int indexCount() {
        AtomicInteger total = new AtomicInteger();
        segmentIndexMatrix.forEach(segmentDataMap -> segmentDataMap.forEach(segment -> total.addAndGet(segment.entryCount())));
        return total.get();
    }

    // TODO(hw):  only data size? how about index size?

    public SegmentIndexRegion getSegmentIndexRegion(int idx, int segIdx) {
        return segmentIndexMatrix.get(idx).get(segIdx);
    }

    public static class SegmentIndexRegion {
        // TODO(hw): id ?
        final int tsCnt;
        final Map<Integer, Integer> tsIdxMap;
        public int ONE_IDX = 0;
        public int NO_IDX = 0;

        // String key reverse order, so it's !SliceComparator
        Map<String, List<Map<Long, Integer>>> keyEntries = new TreeMap<>((a, b) -> -a.compareTo(b));

        public SegmentIndexRegion(int tsCnt, Map<Integer, Integer> tsIdxMap) {
            this.tsCnt = tsCnt;
            this.tsIdxMap = tsIdxMap; // possibly empty
        }

        private void Put(String key, int idxPos, Long time, Integer id) {
            List<Map<Long, Integer>> entryList = keyEntries.getOrDefault(key, new ArrayList<>());
            if (entryList.isEmpty()) {
                for (int i = 0; i < tsCnt; i++) {
                    // !TimeComparator, original TimeComparator is reverse ordered, so here use the default.
                    entryList.add(new TreeMap<>());
                }
                keyEntries.put(key, entryList);
            }
            entryList.get(idxPos).put(time, id);
//            logger.info("after put keyEntries: {}", keyEntries.toString());
        }

        // TODO(hw): return val is only for debug
        public boolean Put(String key, List<Tablet.TSDimension> tsDimensions, Integer dataBlockId) {
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
                    Tablet.TSDimension needPutIdx = tsDimensions.stream().filter(tsDimension -> tsDimension.getIdx() == tsIdx).collect(onlyElement());
                    Put(key, this.ONE_IDX, needPutIdx.getTs(), dataBlockId);
                    put = true;
                }
            } else {
                // tsCnt != 1, KeyEntry array for one key
                for (Tablet.TSDimension tsDimension : tsDimensions) {
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

        public int entryCount() {
            AtomicInteger count = new AtomicInteger();
            keyEntries.forEach((key, keyEntry) -> count.addAndGet(keyEntry.size()));
            return count.get();
        }

        // serialize to `message Segment`
        public Tablet.Segment toProtobuf() {
            Tablet.Segment.Builder builder = Tablet.Segment.newBuilder();

            keyEntries.forEach((key, keyEntry) -> {
                Tablet.Segment.KeyEntries.Builder keyEntriesBuilder = builder.addKeyEntriesBuilder();
                keyEntriesBuilder.setKey(copyFromUtf8(key));
                keyEntry.forEach(timeEntries -> {
                    Tablet.Segment.KeyEntries.KeyEntry.Builder keyEntryBuilder = keyEntriesBuilder.addKeyEntryBuilder();
                    timeEntries.forEach((time, blockId) -> {
                        Tablet.Segment.KeyEntries.KeyEntry.TimeEntry.Builder timeEntryBuilder = keyEntryBuilder.addTimeEntryBuilder();
                        timeEntryBuilder.setTime(time).setBlockId(blockId);
                    });
                });
            });
            return builder.build();
        }
    }

}
