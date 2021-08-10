package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.api.Tablet;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// TODO(hw): run this test instead of running main class, when using system scope dependencies
public class MainTest extends TestCase {

    public void testSegmentIndexRegion() {
        IndexRegionBuilder.SegmentIndexRegion region = new IndexRegionBuilder.SegmentIndexRegion(1, null);
        Map<String, List<Map<Long, Integer>>> treeMap = region.keyEntries;
        // test tree map, should be in reverse order, s11 > s1 > S1
        List<String> keys = Arrays.asList("s11", "s1", "S1");
        keys.forEach(key -> treeMap.put(key, null));
        Assert.assertArrayEquals(keys.toArray(), treeMap.keySet().toArray());
        treeMap.clear();

        // inner tree map, TimeComparator is in desc order, so the reverse order is ascending order.
        List<Long> times = Arrays.asList(1111L, 2222L, 3333L);
        times.forEach(time -> region.Put("s1", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(time).build()), 0));
        Object[] timeArray = treeMap.get("s1").get(0).keySet().toArray();
        Assert.assertArrayEquals(times.toArray(), timeArray);
    }

    public void testHash() {
        List<String> keys = Arrays.asList("2|1", "1|1", "1|4", "2/6", "4", "6", "1");
        for (String key : keys) {
            System.out.println(MurmurHash.hash32(key.getBytes(), key.length(), 0xe17a1465) % 8);
        }
//        c++ hash result
//        I0715 21:37:10.869352 13732 tablet_impl_test.cc:5643] hash(2|1) = 0
//        I0715 21:37:10.869369 13732 tablet_impl_test.cc:5643] hash(1|1) = 2
//        I0715 21:37:10.869380 13732 tablet_impl_test.cc:5643] hash(1|4) = 3
//        I0715 21:37:10.869390 13732 tablet_impl_test.cc:5643] hash(2/6) = 5
//        I0715 21:37:10.869401 13732 tablet_impl_test.cc:5643] hash(4) = 1
//        I0715 21:37:10.869412 13732 tablet_impl_test.cc:5643] hash(6) = 4
//        I0715 21:37:10.869423 13732 tablet_impl_test.cc:5643] hash(1) = 6
    }

    public void testMain() {
        Main.main(null);
    }

}