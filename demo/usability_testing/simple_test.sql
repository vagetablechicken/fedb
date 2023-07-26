create database if not exists simple_test;
use simple_test;
drop table if exists t1;
create table t1(c1 int, c2 string);
set @@execute_mode='online';
insert into t1 values (1, 'a'),(2,'b');
select * from t1;
/**/
deploy d1 select * from t1;
drop deployment d1;
/**/
-- create index -- 等下dl文档
-- drop index
/**/
use simple_test;
set @@execute_mode='offline';
select * from t1;
set @@sync_job=true;
select * from t1;
/**/
show jobs;
drop table t1;
drop database simple_test;