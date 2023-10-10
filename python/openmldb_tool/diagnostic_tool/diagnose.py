#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import os
import textwrap
import time
from collections import defaultdict

from diagnostic_tool.connector import Connector
from diagnostic_tool.dist_conf import read_conf
from diagnostic_tool.conf_validator import (
    DistConfValidator,
    ClusterConfValidator,
)
from diagnostic_tool.log_analyzer import LogAnalyzer
from diagnostic_tool.collector import Collector
import diagnostic_tool.server_checker as checker
from diagnostic_tool.table_checker import TableChecker
from diagnostic_tool.parser import LogParser

from absl import app
from absl import flags
from absl.flags import argparse_flags
from absl import logging  # --verbosity --log_dir

# only some sub cmd needs dist file
flags.DEFINE_string(
    "conf_file",
    "",
    "Cluster config file, supports two styles: yaml and hosts.",
    short_name="f",
)
flags.DEFINE_bool(
    "local",
    False,
    "If set, all server in config file will be treated as local server, skip ssh.",
)
flags.DEFINE_string(
    "db",
    "",
    "Specify databases to diagnose, split by ','. Only used in inspect online.",
)

flags.DEFINE_string("collect_dir", "/tmp/diag_collect", "...")

# ANSI escape codes
flags.DEFINE_bool("nocolor", False, "disable color output", short_name="noc")

# color: red, green
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"


# support values, not just one obj?
def cr_print(color, obj):
    if flags.FLAGS.nocolor or color == None:
        print(obj)
    else:
        print(f"{color}{obj}{RESET}")


def check_version(version_map: dict):
    # cluster must have nameserver, so we use nameserver version to be the right version
    version = version_map["nameserver"][0][1]
    flag = True
    for role, servers in version_map.items():
        for endpoint, cur_version in servers:
            if cur_version != version:
                logging.warning(
                    f"version mismatch. {role} {endpoint} version {cur_version} != {version}"
                )
                flag = False
    return version, flag


def status(args):
    """use OpenMLDB Python SDK to connect OpenMLDB"""
    connect = Connector()
    status_checker = checker.StatusChecker(connect)
    if not status_checker.check_components():
        print("some components is offline")

    # --diff with dist conf file, conf_file is required
    if args.diff:
        assert flags.FLAGS.conf_file, "need --conf_file"
        print(
            "only check components in conf file, if cluster has more components, ignore them"
        )
        dist_conf = read_conf(flags.FLAGS.conf_file)
        assert status_checker.check_startup(
            dist_conf
        ), f"not all components in conf file are online, check the previous output"
        print(f"all components in conf file are online")

    if args.conn:
        status_checker.check_connection()


# TODO: support nocolor
def state2light(state):
    if not state.startswith("k"):
        # meta mismatch status, all red
        return RED + "X" + RESET + " " + state
    else:
        # meta match, get the real state
        state = state[1:]
        if state == "TableNormal":
            # green
            return GREEN + "O" + RESET + " " + state
        else:
            # ref https://github.com/4paradigm/OpenMLDB/blob/0462f8a9682f8d232e8d44df7513cff66870d686/tools/tool.py#L291
            # undefined is loading too state == "kTableLoading" or state == "kTableUndefined"
            # what about state == "kTableLoadingSnapshot" or state == "kSnapshotPaused":
            return YELLOW + "=" + RESET + " " + state


# similar with `show table status` warnings field, but easier to read
# prettytable.colortable just make table border and header lines colorful, so we color the value
def show_table_info(t, replicas_on_tablet, tablet2idx):
    print(
        f"Table {t['tid']} {t['db']}.{t['name']} {t['partition_num']} partitions {t['replica_num']} replicas"
    )
    pnum, rnum = t["partition_num"], t["replica_num"]
    idx_row = []
    leader_row = []
    followers_row = []
    for i, p in enumerate(t["table_partition"]):
        # each partition add 3 row, and rnum + 1 columns
        # tablet idx  pid | 1 | 4 | 5
        # leader       1        o
        # followers         o       o
        pid = p["pid"]
        assert pid == i
        # x.set_style(PLAIN_COLUMNS)
        # sort by list tablets
        replicas = []
        leader = -1
        for r in p["partition_meta"]:
            tablet = r["endpoint"]
            # tablet_has_partition useless?
            # print(r["endpoint"], r["is_leader"], r["tablet_has_partition"])
            replicas_on_t = replicas_on_tablet[t["tid"]][p["pid"]]
            # may can't find replica on tablet, e.g. tablet server is not ready
            info_on_tablet = {}
            if r["endpoint"] not in replicas_on_t:
                info_on_tablet = {"state": "Miss", "mode": "Miss"}
            else:
                info_on_tablet = replicas_on_t[r["endpoint"]]

            m = {
                "role": "leader" if r["is_leader"] else "follower",
                "state": info_on_tablet["state"],
                "acrole": info_on_tablet["mode"],
            }
            replicas.append((tablet2idx[tablet], m))
            if r["is_leader"]:
                leader = len(replicas) - 1
        assert len(replicas) == rnum
        replicas.sort(key=lambda x: x[0])
        # show partition idx and tablet server idx
        idx_row += ["p" + str(pid)] + [r[0] for r in replicas]
        leader_row += [""] * (rnum + 1)
        followers_row += [""] * (rnum + 1)
        cursor = i * (rnum + 1)

        if leader != -1:
            # fulfill leader line
            # append len(replicas) + 1(the first col is empty), and set idx leader to o
            # leader state
            state = replicas[leader][1]["state"]
            if state != "Miss" and replicas[leader][1]["acrole"] != "kTableLeader":
                state = "MetaMismatch"
            leader_row[cursor + leader + 1] = state2light(state)
        else:
            # can't find leader in nameserver metadata TODO: is it ok to set in the first column?
            leader_row[cursor] = state2light("NotFound")

        # fulfill follower line
        for i, r in enumerate(replicas):
            idx = cursor + i + 1
            if i == leader:
                continue
            state = r[1]["state"]
            if state != "Miss" and r[1]["acrole"] != "kTableFollower":
                state = "MetaMismatch"
            followers_row[idx] = state2light(state)
    # TODO: support multi-line for better display?

    from prettytable import PrettyTable

    x = PrettyTable()
    cols = len(idx_row)
    x.field_names = [i for i in range(min(12, cols))]
    # max 12 columns in one row
    step = 12
    for i in range(0, cols, step):
        x.add_row(idx_row[i:i+step])
        x.add_row(leader_row[i:i+step])
        x.add_row(followers_row[i:i+step], divider=True)

    print(x.get_string(border=True, header=False))


def inspect(args):
    # report all
    # 1. server level
    connect = Connector()
    status_checker = checker.StatusChecker(connect)
    server_map = status_checker._get_components()
    print(server_map)
    offlines = []
    for component, value_list in server_map.items():
        for endpoint, status in value_list:
            if status != "online":
                offlines.append(f"[{component}]{endpoint}")
                continue  # offline tablet is needlessly to rpc
    if offlines:
        s = "\n".join(offlines)
        cr_print(RED, f"offline servers:\n{s}")
    else:
        cr_print(GREEN, "all servers online (no backup tm and apiserver)")
    # 2. table level
    rs = connect.execfetch("show table status like '%';")
    rs.sort(key=lambda x: x[0])
    print(f"{len(rs)} tables(including system tables)")
    tid2table = {}
    warn_tables = []
    for t in rs:
        # any warning means unhealthy, partition_unalive may be 0 but already unhealthy, warnings is accurate?
        if t[13]:
            warn_tables.append(f"{t[2]}.{t[1]}")
        tid2table[int(t[0])] = f"{t[2]}.{t[1]}"
    if warn_tables:
        s = "\n".join(warn_tables)
        cr_print(RED, f"unhealthy tables:\n{s}")
    else:
        cr_print(GREEN, "all tables are healthy")
    # 3. partition level
    from diagnostic_tool.rpc import RPC  # TODO 需要调整rpc重要程度，不要optional了？

    # ns table info
    rpc = RPC("ns")
    res = json.loads(rpc.rpc_exec("ShowTable", {"show_all": True}))
    all_table_info = res["table_info"]
    # TODO split system tables and user tables

    # show table info
    # <tid, <pid, <tablet, replica>>>
    replicas = defaultdict(lambda: defaultdict(dict))
    tablets = server_map["tablet"]  # has status
    valid_tablets = set()
    for tablet, status in tablets:
        if status == "offline":
            continue
        # GetTableStatusRequest empty field means get all
        rpc = RPC(tablet)
        res = json.loads(rpc.rpc_exec("GetTableStatus", {}))
        # may get empty when tablet server is not ready
        if "all_table_status" not in res:
            cr_print(RED, f"get failed from {tablet}")
            continue
        valid_tablets.add(tablet)
        for rep in res["all_table_status"]:
            rep["tablet"] = tablet
            # tid, pid are int
            tid, pid = rep["tid"], rep["pid"]
            replicas[tid][pid][tablet] = rep

    tablet2idx = {tablet[0]: i + 1 for i, tablet in enumerate(tablets)}
    print(f"tablet server order: {tablet2idx}")
    print(f"valid tablet servers {valid_tablets}")

    # display, depends on table info, replicas are used to check
    for t in all_table_info:
        show_table_info(t, replicas, tablet2idx)
    # comment for table info display
    print(
        """Notes
        For each partition, the first line is leader, the second line is followers. If all green, the partition is healthy.
        MetaMismatch: nameserver think it's leader/follower, but it's not the role on tabletserver meta.(MisMatch on first line means leader, second line means follower)
        NotFound: replicas in nameserver meta have no leader, only leader line
        Miss: nameserver think it's a replica, but can't find in tabletserver meta.
        """
    )

    # op sorted by id
    print("check all ops in nameserver")
    rs = connect.execfetch("show jobs from NameServer;")
    should_warn = False
    for op in rs:
        if op[2] != "FINISHED":
            print(op)
            should_warn = True
    if not should_warn:
        print("all nameserver ops are finished")

    warning_prefix = """
==================
Warnings:
==================
"""


def insepct_online(args):
    """show table status"""
    conn = Connector()
    # scan all db include system db
    fails = []
    rs = conn.execfetch("show table status like '%';")
    rs.sort(key=lambda x: x[0])
    print(f"inspect {len(rs)} online tables(including system tables)")
    for t in rs:
        if t[13]:
            print(f"unhealthy table {t[2]}.{t[1]}:\n {t[:13]}")
            # sqlalchemy truncated ref https://github.com/sqlalchemy/sqlalchemy/commit/591e0cf08a798fb16e0ee9b56df5c3141aa48959
            # so we print warnings alone
            print(f"full warnings:\n{t[13]}")
            fails.append(f"{t[2]}.{t[1]}")

    assert not fails, f"unhealthy tables: {fails}"
    print(f"all tables are healthy")

    if getattr(args, "dist", False):
        table_checker = TableChecker(conn)
        dbs = flags.FLAGS.db
        db_list = dbs.split(",") if dbs else None
        table_checker.check_distribution(dbs=db_list)


def inspect_offline(args):
    """scan jobs status, show job log if failed"""
    final_failed = ["failed", "killed", "lost"]
    total, num, jobs = _get_jobs(final_failed)
    # TODO some failed jobs are known, what if we want skip them?
    print(f"inspect {total} offline jobs")
    if num:
        failed_jobs_str = "\n".join(jobs)
        raise AssertionError(
            f"{num} offline final jobs are failed\nfailed jobs:\n{failed_jobs_str}"
        )
    print("all offline final jobs are finished")


def _get_jobs(states=None):
    assert checker.StatusChecker(Connector()).offline_support()
    conn = Connector()
    jobs = conn.execfetch("SHOW JOBS")
    total_num = len(jobs)
    # jobs sorted by id
    jobs.sort(key=lambda x: x[0])
    show_jobs = [
        _format_job_row(row) for row in jobs if not states or row[2].lower() in states
    ]
    return total_num, len(show_jobs), show_jobs


def _format_job_row(row):
    row = list(row)
    row[3] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[3] / 1000))
    row[4] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[4] / 1000))
    return " ".join(map(str, row))


def inspect_job(args):
    if not args.id:
        states = args.state.split(",") if args.state != "all" else None
        total, num, jobs = _get_jobs(states)
        print(f"inspect {total} offline jobs")
        if args.state != "all":
            print(f"{num} {args.state} jobs")
        print(*jobs, sep="\n")
        return
    conn = Connector()
    std_output = conn.execfetch(f"SHOW JOBLOG {args.id}")
    assert len(std_output) == 1 and len(std_output[0]) == 1
    detailed_log = std_output[0][0]
    if args.detail:
        print(detailed_log)
    else:
        parser = LogParser()
        if args.conf_update or not os.path.exists(parser.conf_file):
            parser.update_conf_file(args.conf_url)
        parser.parse_log(detailed_log)


def test_sql(args):
    conn = Connector()
    status_checker = checker.StatusChecker(conn)
    if not status_checker.check_components():
        logging.warning("some server is unalive, be careful")
    tester = checker.SQLTester(conn)
    tester.setup()
    print("test online")
    tester.online()
    if status_checker.offline_support():
        print("test offline")
        tester.offline()
    else:
        print("no taskmanager, can't test offline")
    tester.teardown()
    print("all test passed")


def static_check(args):
    assert flags.FLAGS.conf_file, "static check needs dist conf file"
    if not (args.version or args.conf or args.log):
        print("at least one arg to check, check `openmldb_tool static-check -h`")
        return
    dist_conf = read_conf(flags.FLAGS.conf_file)
    # the deploy path of servers may be flags.default_dir, we won't check if it's valid here.
    assert DistConfValidator(dist_conf).validate(), "conf file is invalid"
    collector = Collector(dist_conf)
    if args.version:
        versions = collector.collect_version()
        print(f"version:\n{versions}")  # TODO pretty print
        version, ok = check_version(versions)
        assert ok, f"all servers version should be {version}"
        print(f"version check passed, all {version}")
    if args.conf:
        collector.pull_config_files(flags.FLAGS.collect_dir)
        # config validate, read flags.FLAGS.collect_dir/<server-name>/conf
        if dist_conf.is_cluster():
            assert ClusterConfValidator(dist_conf, flags.FLAGS.collect_dir).validate()
        else:
            assert False, "standalone unsupported"
    if args.log:
        collector.pull_log_files(flags.FLAGS.collect_dir)
        # log check, read flags.FLAGS.collect_dir/logs
        # glog parse & java log
        LogAnalyzer(dist_conf, flags.FLAGS.collect_dir).run()


def rpc(args):
    connect = Connector()
    status_checker = checker.StatusChecker(connect)

    host = args.host
    if not host:
        status_checker.check_components()
        print(
            """choose one host to connect, e.g. "openmldb_tool rpc ns".        
        ns: nameserver(master only, no need to choose)
        tablet:you can get from component table, e.g. the first tablet in table is tablet1
        tm: taskmanager"""
        )
        return
    from diagnostic_tool.rpc import RPC

    # use status connction to get version
    conns_with_version = {
        endpoint: version
        for endpoint, version, _, _ in status_checker.check_connection()
    }
    _, endpoint, _ = RPC.get_endpoint_service(host)
    proto_version = conns_with_version[endpoint]
    print(f"server proto version is {proto_version}")

    operation = args.operation
    field = json.loads(args.field)
    rpc_service = RPC(host)
    if args.hint:
        pb2_dir = flags.FLAGS.pbdir
        print(f"hint use pb2 files from {pb2_dir}")
        # check about rpc depends proto compiled dir
        if (
            not os.path.isdir(pb2_dir)
            or len([pb for pb in os.listdir(pb2_dir) if pb.endswith("_pb2.py")]) < 8
        ):
            print(f"{pb2_dir} is broken, mkdir and download")
            os.system(f"mkdir -p {pb2_dir}")
            import tarfile
            import requests

            # pb2.tar has no dir, extract to pb2_dir
            url = "https://openmldb.ai/download/diag/pb2.tgz"
            r = requests.get(url)
            with open(f"{pb2_dir}/pb2.tgz", "wb") as f:
                f.write(r.content)

            with tarfile.open(f"{pb2_dir}/pb2.tgz", "r:gz") as tar:
                tar.extractall(pb2_dir)
        rpc_service.hint(args.operation)
        return
    if not operation:
        print(
            "choose one operation, e.g. `openmldb_tool rpc ns ShowTable`, --hint for methods list or one method help"
        )
        return
    rpc_service(operation, field)


def parse_arg(argv):
    """parser definition, absl.flags + argparse"""
    parser = argparse_flags.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter
    )
    # use args.header returned by parser.parse_args
    subparsers = parser.add_subparsers(help="OpenMLDB Tool")

    # sub status
    status_parser = subparsers.add_parser(
        "status", help="check the OpenMLDB server status"
    )
    status_parser.add_argument(
        "--diff",
        action="store_true",
        help="check if all endpoints in conf are in cluster. If set, need to set `--conf_file`",
    )  # TODO action support in all python 3.x?
    status_parser.add_argument(
        "--conn",
        action="store_true",
        help="check network connection of all servers",
    )
    status_parser.set_defaults(command=status)

    # sub inspect
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect online and offline. Use `inspect [online/offline]` to inspect one.",
    )
    # inspect online & offline
    inspect_parser.set_defaults(command=inspect)
    inspect_sub = inspect_parser.add_subparsers()
    # inspect online
    online = inspect_sub.add_parser("online", help="only inspect online table.")
    online.set_defaults(command=insepct_online)
    online.add_argument(
        "--dist", action="store_true", help="Inspect online distribution."
    )
    # inspect offline
    offline = inspect_sub.add_parser("offline", help="only inspect offline jobs.")
    offline.set_defaults(command=inspect_offline)
    # inspect job
    ins_job = inspect_sub.add_parser(
        "job", help="show jobs by state, show joblog or parse joblog by id."
    )
    ins_job.set_defaults(command=inspect_job)
    ins_job.add_argument(
        "--state", default="all", help="Specify which state offline jobs, split by ','"
    )
    ins_job.add_argument("--id", help="inspect joblog by id")
    ins_job.add_argument(
        "--detail",
        action="store_true",
        help="show detailed joblog information, use with `--id`",
    )
    ins_job.add_argument(
        "--conf-url",
        default="https://raw.githubusercontent.com/4paradigm/OpenMLDB/main/python/openmldb_tool/diagnostic_tool/common_err.yml",
        help="url used to update the log parser configuration. If downloading is slow, you can try mirror source 'https://openmldb.ai/download/diag/common_err.yml'",
    )
    ins_job.add_argument(
        "--conf-update", action="store_true", help="update the log parser configuration"
    )

    # sub test
    test_parser = subparsers.add_parser(
        "test",
        help="Do simple create&insert&select test in online, select in offline(if taskmanager exists)",
    )
    test_parser.set_defaults(command=test_sql)

    # sub static-check
    static_check_parser = subparsers.add_parser(
        "static-check",
        help=textwrap.dedent(
            """ \
        Static check on remote host, version/conf/log, -h to show the arguments, --conf_file is required.
        Use -VCL to check all.
        You can check version or config before cluster running.
        If servers are remote, need Passwordless SSH Login.
        """
        ),
    )
    static_check_parser.add_argument(
        "--version", "-V", action="store_true", help="check version"
    )
    static_check_parser.add_argument(
        "--conf", "-C", action="store_true", help="check conf"
    )
    static_check_parser.add_argument(
        "--log", "-L", action="store_true", help="check log"
    )
    static_check_parser.set_defaults(command=static_check)

    # sub rpc
    rpc_parser = subparsers.add_parser(
        "rpc",
        help="user-friendly rpc tool",
    )
    rpc_parser.add_argument(
        "host",
        nargs="?",
        help=textwrap.dedent(
            """ \
        host name, if no value, print the component table. 
        ns: nameserver(master only, no need to choose)
        tablet:you can get from component table, e.g. the first tablet in table is tablet1
        tm: taskmanager
        """
        ),
    )
    rpc_parser.add_argument(
        "operation",
        nargs="?",
        default="",
    )
    rpc_parser.add_argument(
        "--field",
        default="{}",
        help='json format, e.g. \'{"db":"db1","table":"t1"}\', default is \'{}\'',
    )
    rpc_parser.add_argument(
        "--hint",
        action="store_true",
        help="print rpc hint for current operation(rpc method), if no operation, print all possible rpc methods",
    )
    rpc_parser.set_defaults(command=rpc)

    def help(args):
        parser.print_help()

    parser.set_defaults(command=help)

    args = parser.parse_args(argv[1:])
    tool_flags = {
        k: [flag.serialize() for flag in v]
        for k, v in flags.FLAGS.flags_by_module_dict().items()
        if "diagnostic_tool" in k
    }
    logging.debug(f"args:{args}, flags: {tool_flags}")

    return args


def main(args):
    # TODO: adjust args, e.g. if conf_file, we can get zk addr from conf file, no need to --cluster
    # run the command
    print(f"diagnosing cluster {Connector().address()}")
    args.command(args)


def run():
    app.run(main, flags_parser=parse_arg)


if __name__ == "__main__":
    app.run(main, flags_parser=parse_arg)
