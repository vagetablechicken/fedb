package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.api.Tablet;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataRegionBuilderTest extends TestCase {
    private static final Logger logger = LoggerFactory.getLogger(DataRegionBuilderTest.class);

    public void testNormalSizeLimit() throws IOException {
        // small size limit for testing, so we just set it to 32B.
        DataRegionBuilder dataRegionBuilder = new DataRegionBuilder(1, 1, 32);
        ByteBuffer bufferSample = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            dataRegionBuilder.addDataBlock(bufferSample, 0);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(false, stream);
            if (request != null) {
                logger.info("after add block {}, request size={}, attachment size={}", i, request.getSerializedSize(), stream.size());
            }
        }
    }

}