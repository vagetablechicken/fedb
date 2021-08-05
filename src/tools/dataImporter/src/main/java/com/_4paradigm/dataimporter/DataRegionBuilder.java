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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataRegionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(DataRegionBuilder.class);

    private final int tid;
    private final int pid;
    private final int rpcSizeLimit;

    private final List<Tablet.DataBlockInfo> dataBlockInfoList = new ArrayList<>(); // TODO(hw): to queue?
    private final List<ByteBuffer> dataList = new ArrayList<>(); // TODO(hw): to queue?
    private int absoluteDataLength = 0;
    private int absoluteNextId = 0;
    private int estimatedTotalSize = 0;
    public static final int reqReservedSize = 6; // TODO(hw): 6 is tid/pid/partid, each cost 2B.
    public static final int estimateInfoSize = 8; // Tablet.DataBlockInfo 6B, but it'll be added into the list, more 2B.

    private int partId = 0;

    // RPC sizeLimit won't be changed frequently, so it could be final.
    public DataRegionBuilder(int tid, int pid, int rpcSizeLimit) {
        this.tid = tid;
        this.pid = pid;
        this.rpcSizeLimit = rpcSizeLimit;
    }

    // used by IndexRegion
    public int nextId() {
        return absoluteNextId;
    }

    // After data region all sent, index region RPCs need start after the last part, loader(TabletServer) will check the order.
    public int getNextPartId() {
        return partId;
    }

    public void addDataBlock(ByteBuffer data, int refCnt) {
        int head = absoluteDataLength;
        dataList.add(data);
        int length = data.limit();
        Tablet.DataBlockInfo info = Tablet.DataBlockInfo.newBuilder().setRefCnt(refCnt).setOffset(head).setLength(length).build();
        dataBlockInfoList.add(info);
        absoluteDataLength += length;
        absoluteNextId++;
        estimatedTotalSize += estimateInfoSize + length;
        logger.info("after size {}(exclude header 6)", estimatedTotalSize);
    }

    public Tablet.BulkLoadRequest buildPartialRequest(boolean force, ByteArrayOutputStream attachmentStream) throws IOException {
        // if current size < sizeLimit, no need to send.
        if (!force && reqReservedSize + estimatedTotalSize <= rpcSizeLimit) {
            return null;
        }
        // To limit the size properly, request message + attachment will be <= rpcSizeLimit
        // We need to add data blocks one by one.

        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(tid).setPid(pid);

        int shouldBeSentEnd = 0;
        int sentTotalSize = 0;
        attachmentStream.reset();
        for (int i = 0; i < dataBlockInfoList.size(); i++) {
            int add = dataBlockInfoList.get(i).getLength() + estimateInfoSize;
            if (sentTotalSize + add > rpcSizeLimit) {
                break;
            }
            builder.addBlockInfo(dataBlockInfoList.get(i));
            attachmentStream.write(dataList.get(i).array());
            sentTotalSize += add;
            shouldBeSentEnd++;
        }

        // clear sent data [0..End)
        logger.debug("sent {} data blocks, remain {} blocks", shouldBeSentEnd, dataBlockInfoList.size() - shouldBeSentEnd);
        dataBlockInfoList.subList(0, shouldBeSentEnd).clear();
        dataList.subList(0, shouldBeSentEnd).clear();
        estimatedTotalSize -= (estimateInfoSize * shouldBeSentEnd + attachmentStream.size());
        // TODO(hw): for debug
        logger.info("estimate data rpc size = {}", estimateInfoSize * shouldBeSentEnd + attachmentStream.size());
        builder.setPartId(partId);

        if (force && shouldBeSentEnd == 0) {
            return null;
        }
        partId++;
        return builder.build();
    }
}
