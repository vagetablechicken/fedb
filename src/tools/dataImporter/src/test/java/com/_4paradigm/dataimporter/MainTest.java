package com._4paradigm.dataimporter;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// TODO(hw): run this test instead of running main class, when using system scope dependencies
public class MainTest extends TestCase {

    public void testBulkLoad() {
        List<String> keys = Arrays.asList("2|1", "1|1", "1|4", "2/6", "4", "6", "1");
        for (String key : keys) {
            System.out.println(Main.hash(key.getBytes(), key.length(), 0xe17a1465)%8);
        }
//        I0715 21:37:10.869352 13732 tablet_impl_test.cc:5643] hash(2|1) = 0
//        I0715 21:37:10.869369 13732 tablet_impl_test.cc:5643] hash(1|1) = 2
//        I0715 21:37:10.869380 13732 tablet_impl_test.cc:5643] hash(1|4) = 3
//        I0715 21:37:10.869390 13732 tablet_impl_test.cc:5643] hash(2/6) = 5
//        I0715 21:37:10.869401 13732 tablet_impl_test.cc:5643] hash(4) = 1
//        I0715 21:37:10.869412 13732 tablet_impl_test.cc:5643] hash(6) = 4
//        I0715 21:37:10.869423 13732 tablet_impl_test.cc:5643] hash(1) = 6
        Main.main(null);
    }

}