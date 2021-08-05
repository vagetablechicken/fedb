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
    private int currentTotalSize = 0;
    private int shouldBeSentEnd = 0; // idx in dataBlockInfoList -> [0...end) should be sent.
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

    public void addDataBlock(ByteBuffer data, int refCnt) {
        int head = absoluteDataLength;
        dataList.add(data);
        int length = data.limit();
        Tablet.DataBlockInfo info = Tablet.DataBlockInfo.newBuilder().setRefCnt(refCnt).setOffset(head).setLength(length).build();
        dataBlockInfoList.add(info);
        absoluteDataLength += length;
        absoluteNextId++;

        currentTotalSize += info.getSerializedSize() + length;
        logger.info("test info size {}, data length {}, current total size {}", info.getSerializedSize(), length, currentTotalSize);
        if (currentTotalSize <= rpcSizeLimit) {
            shouldBeSentEnd = dataBlockInfoList.size();
        }
    }

    public Tablet.BulkLoadRequest buildPartialRequest(boolean force, ByteArrayOutputStream attachmentStream) throws IOException {
        // if current size < sizeLimit, no need to send.
        if (!force && currentTotalSize <= rpcSizeLimit) {
            return null;
        }
        // To limit the size properly, request message + attachment will be <= rpcSizeLimit, it's recorded by `shouldBeSentEnd`.

        Preconditions.checkState(!(shouldBeSentEnd == 0 && currentTotalSize != 0),
                "contains one too big data block, can't build < {} rpc", rpcSizeLimit);

        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(tid).setPid(pid);
        // [0..end) should be sent.
        builder.addAllBlockInfo(dataBlockInfoList.subList(0, shouldBeSentEnd));
        Preconditions.checkState(builder.getBlockInfoCount() == shouldBeSentEnd);

        for (int i = 0; i < shouldBeSentEnd; i++) {
            // if throw Exception, let upper level know
            attachmentStream.write(dataList.get(i).array());
        }

        // clear sent data
        dataBlockInfoList.subList(0, shouldBeSentEnd).clear();
        dataList.subList(0, shouldBeSentEnd).clear();
        shouldBeSentEnd = 0;

        builder.setDataPartId(partId);
        partId++;
        return builder.build();
    }
}
