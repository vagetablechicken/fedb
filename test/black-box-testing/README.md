# Black-box testing

## Introduction

This directory contains the black-box testing for the project. The tests are sql scripts that are run against the database. But some of the tests need setup and the setup may can not be done automatically.

If use diag tool to run the tests, we can check the result of the tests automatically. If run the tests directly, we need to check the result manually, grep `ERROR` in results.
不做什么正确性检查，只检查是否有错误，有错误就是失败。正确性是别的测试的事情，这里只在意**用户的日常操作是否会引出问题**，不要去解决问题。
## simple/
This directory contains the simple tests. The tests are simple and can be run automatically. No setup needed.

```
create database simple_test;
use simple_test;
create table t1(c1 int, c2 string);
set @@execute_mode='online';
insert into t1 values (1, 'a'),(2,'b');
select * from t1;
```
TODO add deployment test here.
deploy
drop
deploy

create index
drop index

If offline supported, the following test can be added. Offline data needs to be setup manually, so test empty table here.
```
set @@execute_mode='offline';
select * from t1;
set @@sync_job=true;
select * from t1;
```

Teardown:


## offline/
This directory contains the offline tests. The tests are simple and can be run automatically. But it needs setup, may needs to be done manually.

source:
- Local spark and file, `file://` is used in the test. And we assume the taskmanager process is running on the same machine as the test, and the spark master is local. Distributed cluster should use hdfs path.
- ...

Setup: generate data is more flexible, if load met error, we can gen a smaller data set to compare.
```
# generate data, multi files, big file e.g. 1G/512M, to meet executor memory limit
/tmp/openmldb_test/t1/*.csv
/tmp/openmldb_test/t2/*.parquet
```
If hive, give hive source path, and dst path, plus create table like hive
If hdfs, give hdfs tmp path, 给予权限，cp data to hdfs tmp path, 目录使用tmp_path/t1，而dst tmp_path/dst...可以主动创建
If local, use /tmp/openmldb_test path

Run(no matter source type):
```
create database offline_test;
use offline_test;
create table t1(c1 int, c2 string);
set @@execute_mode='offline';
# TODO csv has more options to test, soft/deep
load data infile '<src_path>/t1' into table t1 options();
set @@sync_job=false;
select * from t1;
set @@sync_job=true;
select * from t1;
create table t2 like parquet...;
# TODO soft/deep
set @@sync_job=true;
load data infile '<src_path>/t2' into table t2 options(format='parquet');
set @@sync_job=false;
select * from t2;
set @@sync_job=true;
select * from t2;

alter table ...;

select * from t1 into outfile '<dst_path>/t1' options();
drop table t1;
drop table t2;
drop database offline_test;
```

不检查不容易发现问题？还是得有检查，比如load后select into。无论最后是手动还是自动检查dst数据。避免之前出现过的离线数据加载问题。就检查末尾，不去检查中间环节。黑盒。

yarn spark
stop job

## udf/

Setup:
download udf
docker-cp by diag
manual-cp by scp or other tools, sbin can't do it?

Run:
```
create database udf_test;
use udf_test;
create table t1(c1 int, c2 string);
create function ...;
set @@execute_mode='offline';
select udf(c1) from t1;
set @@execute_mode='online';
select udf(c1) from t1;
drop function ...;
drop table t1;
drop database udf_test;
```
