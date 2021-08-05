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
import junit.framework.TestCase;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataRegionBuilderTest extends TestCase {
    private static final Logger logger = LoggerFactory.getLogger(DataRegionBuilderTest.class);

    public void testRequestSize() {
        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(1).setPid(1);
        builder.setPartId(0);
        Assert.assertEquals(6, builder.build().getSerializedSize());
        for (int i = 0; i < 10; i++) {
            Tablet.DataBlockInfo info = Tablet.DataBlockInfo.newBuilder().setRefCnt(i).setOffset(i).setLength(i).build();
            Assert.assertEquals(DataRegionBuilder.reqReservedSize, info.getSerializedSize());
            builder.addBlockInfo(info);
            Assert.assertEquals(DataRegionBuilder.reqReservedSize + (i + 1) * DataRegionBuilder.estimateInfoSize,
                    builder.build().getSerializedSize());
        }
    }

    public void testNormalSizeLimit() throws IOException {
        // small size limit for testing, so we just set it to 32B.
        DataRegionBuilder dataRegionBuilder = new DataRegionBuilder(1, 1, 32);
        ByteBuffer bufferSample = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            dataRegionBuilder.addDataBlock(bufferSample, 0);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(false, stream);
            // each row add 18B, so we will get a request that has only one row.
            Assert.assertNotNull(request);
            Assert.assertEquals(DataRegionBuilder.reqReservedSize + DataRegionBuilder.estimateInfoSize, request.getSerializedSize());
            Assert.assertEquals(bufferSample.limit(), stream.size());
        }
        // remain 1 row
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(true, stream);
    }

}