# Diag Tool

In `diagnostic_tool/`:

main: diagnose.py

ssh/scp by connections.py

read distribution yaml/hosts by dist_conf.py

## Usage

core commands:
status
inspect   [online] check all table status
          [offline] offline jobs status
          no sub means all?
test   test online insert&select, test offline select if taskmanager
remote needs config file(dist.yml or hosts)

openmldb_tool status --cluster=127.0.0.1:2181/openmldb
```
status [-h] [--helpfull] [--diff DIFF]

optional arguments:
  -h, --help   show this help message and exit
  --helpfull   show full help message and exit
  --diff DIFF  check if all endpoints in conf are in cluster, true/false. If true, need to set `--conf_file`
```

show table status, get just one table? and check the hidden db tables status
  show table status can detect the problem? `Fail to get tablet from cache` 

## Collector

collector.py collects config, log and version

TODO: `<cluster-name>-conf` is better than custom dest name?

### config
```
<dest>/
  <ip:port>-nameserver/
    nameserver.flags
  <ip:port>-tablet/
    tablet.flags
  <ip:port>-tablet/
    tablet.flags
  <ip:port>-taskmanager/
    taskmanager.properties
```

### log
Find log path in remote config file.

Get last 2 files.

```
<dest>/
  <ip:port>-nameserver/
    nameserver.info.log.1
    nameserver.info.log.2
    ...
  <ip:port>-tablet/
    ...
  <ip:port>-taskmanager/
    taskmanager.log.1
    job_1_error.log
    ...
```

### version

exec openmldb

run jar taskmanager and batch

#### find batch jar
find spark home from remote taskmanager config file.

## analysis

log_analysis.py read logs from local path `<dest>`. 

NOTE: if diag local cluster/standalone, directory structure is different.
