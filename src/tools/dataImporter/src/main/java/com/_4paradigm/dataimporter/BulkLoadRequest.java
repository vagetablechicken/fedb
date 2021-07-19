package com._4paradigm.dataimporter;

import com._4paradigm.fedb.api.API;
import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.protobuf.ByteString.copyFromUtf8;

public class BulkLoadRequest {
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

}
