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

from diagnostic_tool.connector import Connector
from diagnostic_tool.dist_conf import ConfParser, read_conf
from diagnostic_tool.conf_validator import (
    ConfValidator,
    StandaloneConfValidator,
    ClusterConfValidator,
    TaskManagerConfValidator,
)
from diagnostic_tool.log_analysis import LogAnalysis
from diagnostic_tool.collector import Collector
import diagnostic_tool.server_checker as checker

from absl import app
from absl import flags
from absl.flags import argparse_flags
from absl import logging  # --verbosity --log_dir

# only some sub cmd needs dist file
flags.DEFINE_string(
    "conf_file",
    "",
    "Cluster config file, supports two styles: yaml and hosts. ",
    short_name="f",
)


def check_version(version_map: dict):
    f_version = ""
    f_endpoint = ""
    f_role = ""
    flag = True
    for k, v in version_map.items():
        for endpoint, cur_version in v:
            if f_version == "":
                f_version = cur_version
                f_endpoint = endpoint
                f_role = k
            if cur_version != f_version:
                logging.warn(
                    f"version mismatch. {k} {endpoint} version {cur_version}, {f_role} {f_endpoint} version {f_version}"
                )
                flag = False
    return flag, f_version


def check_conf(yaml_conf_dict, conf_map):
    detail_conf_map = {}
    flag = True
    for role, v in conf_map.items():
        for endpoint, values in v.items():
            for _, path in values:
                detail_conf_map.setdefault(role, [])
                cur_conf = ConfParser(path).conf()
                detail_conf_map[role].append(cur_conf)
                if yaml_conf_dict["mode"] == "cluster" and role == "taskmanager":
                    taskmanager_validator = TaskManagerConfValidator(cur_conf)
                    if not taskmanager_validator.validate():
                        logging.warn(f"taskmanager {endpoint} conf check failed")
                        flag = False

    if yaml_conf_dict["mode"] == "standalone":
        conf_validator = StandaloneConfValidator(
            detail_conf_map["nameserver"][0], detail_conf_map["tablet"][0]
        )
    else:
        conf_validator = ClusterConfValidator(yaml_conf_dict, detail_conf_map)
    if conf_validator.validate() and flag:
        logging.info("check conf ok")
    else:
        logging.warn("check conf failed")


def check_log(yaml_conf_dict, log_map):
    flag = True
    for role, v in log_map.items():
        for endpoint, values in v.items():
            log_analysis = LogAnalysis(role, endpoint, values)
            if not log_analysis.analysis_log():
                flag = False
    if flag:
        logging.info("check logging ok")


# def main(argv):
#     conf_opt = ConfOption()
#     if not conf_opt.init():
#         return
#     util.clean_dir(conf_opt.data_dir)
#     dist_conf = YamlConfReader(conf_opt.dist_conf).conf()
#     yaml_validator = YamlConfValidator(dist_conf.full_conf)
#     if not yaml_validator.validate():
#         logging.warning("check yaml conf failed")
#         sys.exit()
#     logging.info("check yaml conf ok")

#     logging.info("mode is {}".format(dist_conf.mode))



def status(args):
    """use OpenMLDB Python SDK to connect OpenMLDB"""
    conn = Connector()
    status_checker = checker.StatusChecker(conn)
    assert status_checker.check_components()

    # --diff with dist conf file, conf_file is required
    if args.diff:
        assert flags.FLAGS.conf_file
        status_checker.check_startup("")  # TODO


def inspect(args):
    insepct_online(args)
    inspect_offline(args)


def insepct_online(args):
    """show table status"""
    conn = Connector()
    # scan all db include system db
    # INTERNAL_DB
    # INFORMATION_SCHEMA_DB
    # PRE_AGG_DB
    dbs = ["__INTERNAL_DB", "INFORMATION_SCHEMA", "__PRE_AGG_DB"] + [
        r[0] for r in conn.execfetch("SHOW DATABASES")
    ]
    for db in dbs:
        conn.execute(f"USE {db}")
        rs = conn.execfetch("SHOW TABLE STATUS")
        for t in rs:
            if t[13]:
                print(f"unhealthy table {t[2]}.{t[1]}:\n {t[:13]}")
                print(
                    f"full warnings:\n{t[13]}"
                )  # sqlalchemy truncated ref https://github.com/sqlalchemy/sqlalchemy/commit/591e0cf08a798fb16e0ee9b56df5c3141aa48959


def inspect_offline(args):
    """"""
    assert checker.StatusChecker(Connector()).offline_support()
    conn = Connector()
    jobs = conn.execfetch("SHOW JOBS")
    print(f"inspect {len(jobs)} jobs")
    has_failed = False
    for row in jobs:
        if row[2] != "FINISHED":
            has_failed = True
            std_output = conn.execfetch(f"SHOW JOBLOG {row[0]}")
            print(f"{row[0]}-{row[1]} failed, job log:\n{std_output}")
    assert not has_failed


def test_sql(args):
    conn = Connector()
    status_checker = checker.StatusChecker(conn)
    if not status_checker.check_components():
        logging.warning("some server is unalive, be careful")
    tester = checker.SQLTester(conn)
    tester.setup()
    tester.online()
    if status_checker.offline_support():
        tester.offline()
    else:
        print("no taskmanager, can't test offline")
    tester.teardown()


def static_check(args):
    assert flags.FLAGS.conf_file, "static check needs dist conf file"
    conf = read_conf(flags.FLAGS.conf_file)
    assert ConfValidator(conf.full_conf).validate(), "conf file is invalid"
    collector = Collector(conf)
    if args.version:
        pass
    if args.conf:
        pass
    if args.log:
        pass
#     if dist_conf.mode == "cluster" and conf_opt.env != "onebox":
#         collector = Collector(dist_conf)
#         if conf_opt.check_version():
#             version_map = collector.collect_version()
#         if conf_opt.check_conf():
#             collector.pull_config_files(f"{conf_opt.data_dir}/conf")
#         if conf_opt.check_log():
#             collector.pull_log_files(f"{conf_opt.data_dir}/logging")
#         if conf_opt.check_conf() or conf_opt.check_log():
#             file_map = util.get_files(conf_opt.data_dir)
#             logging.debug("file_map: %s", file_map)
#     else:
#         collector = LocalCollector(dist_conf)
#         if conf_opt.check_version():
#             version_map = collector.collect_version()
#         if conf_opt.check_conf() or conf_opt.check_log():
#             file_map = collector.collect_files()
#             logging.debug("file_map: %s", file_map)

#     if conf_opt.check_version():
#         flag, version = check_version(version_map)
#         if flag:
#             logging.info(f"openmldb version is {version}")
#             logging.info("check version ok")
#         else:
#             logging.warn("check version failed")

#     if conf_opt.check_conf():
#         check_conf(dist_conf.full_conf, file_map["conf"])
#     if conf_opt.check_log():
#         check_log(dist_conf.full_conf, file_map["logging"])

def parse_arg(argv):
    """parser definition"""
    parser = argparse_flags.ArgumentParser()
    # use args.header returned by parser.parse_args
    subparsers = parser.add_subparsers(help="OpenMLDB Tool")
    # sub status
    status_parser = subparsers.add_parser(
        "status", help="check the OpenMLDB server status"
    )
    status_parser.add_argument(
        "--diff",
        action="store_true",
        help="check if all endpoints in conf are in cluster, true/false. If true, need to set `--conf_file`",
    )  # TODO action support version?
    status_parser.set_defaults(command=status)

    # sub test
    test_parser = subparsers.add_parser(
        "test", help="do simple create&insert test, what about offline?"
    )
    test_parser.set_defaults(command=test_sql)

    # sub inspect
    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect online and offline. Use `inspect [online/offline]` to inspect one. Support table status later",
    )
    # inspect online & offline
    inspect_parser.set_defaults(command=inspect)
    inspect_sub = inspect_parser.add_subparsers()
    # inspect online
    online = inspect_sub.add_parser("online", help="only inspect online table")
    online.set_defaults(command=insepct_online)
    # inspect offline
    offline = inspect_sub.add_parser(
        "offline", help="only inspect offline jobs, check the job log"
    )
    offline.set_defaults(command=inspect_offline)

    # sub remote
    static_check_parser = subparsers.add_parser(
        "static-check",
        help="static check on remote host, version/conf/log, need --conf_file, and if remote, need Passwordless SSH Login",
    )
    static_check_parser.add_argument(
        "-V", "--version", action="store_true", help="check version"
    )
    static_check_parser.add_argument(
        "-C", "--conf", action="store_true", help="check conf"
    )
    static_check_parser.add_argument(
        "-L", "--log", action="store_true", help="check log"
    )
    static_check_parser.set_defaults(command=static_check)

    args = parser.parse_args(argv)
    tool_flags = {
        k: [flag.serialize() for flag in v]
        for k, v in flags.FLAGS.flags_by_module_dict().items()
        if "diagnostic_tool" in k
    }
    logging.info(f"args:{args}, flags: {tool_flags}")
    return args


def main1(argv):
    args = parse_arg(argv)
    # run the command
    args.command(args)


def run():
    app.run(main1)


if __name__ == "__main__":
    app.run(main1)
