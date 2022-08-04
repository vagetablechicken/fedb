main: diagnose.py

ssh/scp by connections.py

read distribution yaml by dist_conf.py

collector.py collects config, log, ...

TODO: `<cluster-name>-conf` is better than custom dest name?

config:
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

log:
```
<dest>/
  <ip:port>-nameserver/
    nameserver.info.log.1
    nameserver.info.log.2
    ...
  <ip:port>-tablet/
    ...
  <ip:port>-taskmanager/
    ...
```

versions:
exec ...

TODO: taskmanager version(not only the leader)
 
how to get one taskmanager version in local?

log_analysis.py read logs from local path `<dest>`.
