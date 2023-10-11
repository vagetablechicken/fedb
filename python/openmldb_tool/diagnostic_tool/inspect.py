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

""" gen multi-level readable reports for cluster devops """
from absl import flags
import json
from collections import defaultdict
from prettytable import PrettyTable

from .rpc import RPC

# ANSI escape codes
flags.DEFINE_bool("nocolor", False, "disable color output", short_name="noc")
flags.DEFINE_integer("table_width", 12, "max columns in one row", short_name="tw")

# color: red, green
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"


# switch by nocolor flag
def cr_print(color, obj):
    if flags.FLAGS.nocolor or color == None:
        print(obj)
    else:
        print(f"{color}{obj}{RESET}")


def server_ins(server_map):
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


# support nocolor
def light(color, symbol, detail):
    if flags.FLAGS.nocolor:
        return f"{symbol} {detail}"
    else:
        return f"{color}{symbol}{RESET} {detail}"


def state2light(state):
    if not state.startswith("k"):
        # meta mismatch status, all red
        return light(RED, "X", state)
    else:
        # meta match, get the real state
        state = state[1:]
        if state == "TableNormal":
            # green
            return light(GREEN,"O",state)
        else:
            # ref https://github.com/4paradigm/OpenMLDB/blob/0462f8a9682f8d232e8d44df7513cff66870d686/tools/tool.py#L291
            # undefined is loading too state == "kTableLoading" or state == "kTableUndefined"
            # what about state == "kTableLoadingSnapshot" or state == "kSnapshotPaused": TODO
            return light(YELLOW ,"=" , state)


# similar with `show table status` warnings field, but easier to read
# prettytable.colortable just make table border and header lines colorful, so we color the value
def show_table_info(t, replicas_on_tablet, tablet2idx):
    print(
        f"Table {t['tid']} {t['db']}.{t['name']} {t['partition_num']} partitions {t['replica_num']} replicas"
    )
    pnum, rnum = t["partition_num"], t["replica_num"]
    assert pnum == len(t["table_partition"])

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

        # sort by list tablets
        replicas = []
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

        assert len(replicas) == rnum
        replicas.sort(key=lambda x: x[0])
        leader_ind = [i for i, r in enumerate(replicas) if r[1]["role"] == "leader"]
        # replica on offline tablet is still in the ns meta, so leader may > 1
        # assert len(ind) <= 1, f"should be only one leader or miss leader in {replicas}"

        # show partition idx and tablet server idx
        idx_row += ["p" + str(pid)] + [r[0] for r in replicas]
        leader_row += [""] * (rnum + 1)
        followers_row += [""] * (rnum + 1)
        cursor = i * (rnum + 1)
        # fulfill leader line
        if leader_ind:
            for leader in leader_ind:
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
            if i in leader_ind:
                continue
            state = r[1]["state"]
            if state != "Miss" and r[1]["acrole"] != "kTableFollower":
                state = "MetaMismatch"
            followers_row[idx] = state2light(state)


    x = PrettyTable()
    cols = len(idx_row)
    # multi-line for better display, max 12 columns in one row 
    x.field_names = [i for i in range(min(flags.FLAGS.table_width, cols))]
    step = flags.FLAGS.table_width
    for i in range(0, cols, step):
        x.add_row(idx_row[i : i + step])
        x.add_row(leader_row[i : i + step])
        x.add_row(followers_row[i : i + step], divider=True)

    print(x.get_string(border=True, header=False))


def table_ins(connect):
    rs = connect.execfetch("show table status like '%';")
    rs.sort(key=lambda x: x[0])
    print(f"{len(rs)} tables(including system tables)")
    warn_tables = []
    for t in rs:
        # any warning means unhealthy, partition_unalive may be 0 but already unhealthy, warnings is accurate?
        if t[13]:
            warn_tables.append(f"{t[2]}.{t[1]}")
    if warn_tables:
        s = "\n".join(warn_tables)
        cr_print(RED, f"unhealthy tables:\n{s}")
    else:
        cr_print(GREEN, "all tables are healthy")
    return warn_tables

def partition_ins(server_map):
    # ns table info
    rpc = RPC("ns")
    res = rpc.rpc_exec("ShowTable", {"show_all": True})
    if not res:
        cr_print(RED, "get table info failed or empty from nameserver")
        return
    res = json.loads(res)
    all_table_info = res["table_info"]
    # TODO split system tables and user tables

    # get table info from tablet server
    # <tid, <pid, <tablet, replica>>>
    replicas = defaultdict(lambda: defaultdict(dict))
    tablets = server_map["tablet"]  # has status
    invalid_tablets = set()
    for tablet, status in tablets:
        if status == "offline":
            invalid_tablets.add(tablet)
            continue
        # GetTableStatusRequest empty field means get all
        rpc = RPC(tablet)
        res = json.loads(rpc.rpc_exec("GetTableStatus", {}))
        # may get empty when tablet server is not ready
        if not res or res["code"] != 0:
            cr_print(RED, f"get table status failed or empty from {tablet}(online)")
            invalid_tablets.add(tablet)
            continue
        if "all_table_status" not in res:
            # just empty replica on tablet, skip
            continue
        for rep in res["all_table_status"]:
            rep["tablet"] = tablet
            # tid, pid are int
            tid, pid = rep["tid"], rep["pid"]
            replicas[tid][pid][tablet] = rep

    tablet2idx = {tablet[0]: i + 1 for i, tablet in enumerate(tablets)}
    print(f"tablet server order: {tablet2idx}")
    # print(f"valid tablet servers {valid_tablets}")
    if invalid_tablets:
        cr_print(
            RED,
            f"some tablet servers are offline/bad, can't get table info(exclude empty table server): {invalid_tablets}",
        )

    # display, depends on table info, replicas are used to check
    all_table_info.sort(key=lambda x: x["tid"])
    for t in all_table_info:
        # TODO: no need to print healthy table
        show_table_info(t, replicas, tablet2idx)
    # comment for table info display TODO: 
    print(
        """Notes
        For each partition, the first line is leader, the second line is followers. If all green, the partition is healthy.
        MetaMismatch: nameserver think it's leader/follower, but it's not the role on tabletserver meta.(MisMatch on first line means leader, second line means follower)
        NotFound: replicas in nameserver meta have no leader, only leader line
        Miss: nameserver think it's a replica, but can't find in tabletserver meta.
        """
    )

def ops_ins(connect):
    # op sorted by id TODO: detail to show all?
    print("show last 10 ops which are not finished")
    rs = connect.execfetch("show jobs from NameServer;")
    should_warn = []
    from datetime import datetime

    for op in rs:
        op = list(op)
        if op[2] != "FINISHED":
            op[3] = str(datetime.fromtimestamp(int(op[3]) / 1000))
            op[4] = str(datetime.fromtimestamp(int(op[4]) / 1000))
            should_warn.append(op)
    if not should_warn:
        print("all nameserver ops are finished")
    else:
        print("last 10 unfinished ops:")
        print(*should_warn[-10:], sep="\n")

def inspect_hint(): # TODO:
    print( """
==================
Summary&Hint:
==================
If partition check get error, you can try recoverdata....
"""
    )