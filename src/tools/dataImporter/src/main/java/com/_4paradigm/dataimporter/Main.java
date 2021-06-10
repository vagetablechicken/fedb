package com._4paradigm.dataimporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // src: file path
        // input way: config file, SQL "LOAD DATA INFILE", may needs hdfs sasl config

        // reader, perhaps needs hdfs writer later
        // reader can read dir | file | *.xx?
        // support range? -> to use multi-threads

        // ETL?

        // dst: cluster name, db & table name
        // write to dst, by sdk, what about one row failed? ——关系到什么操作是原子的，以及如何组织插入，比如是否支持指定多路径对
        // 一张表，即使用户是散开写的，我们可以整合？如果用单条put的话，好像没啥意义。

        logger.info("main");
    }
}
