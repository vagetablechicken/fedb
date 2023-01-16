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
from diagnostic_tool.collector import Collector, LocalCollector
from diagnostic_tool.dist_conf import YamlConfReader, ConfParser, DistConf
from diagnostic_tool.conf_validator import YamlConfValidator, StandaloneConfValidator, ClusterConfValidator, TaskManagerConfValidator
from diagnostic_tool.log_analysis import LogAnalysis
import diagnostic_tool.server_checker as checker
import diagnostic_tool.util as util
import sys

from absl import app
from diagnostic_tool.conf_option import ConfOption
from absl import flags
from absl.flags import argparse_flags
from absl import logging # --logger_levels --log_dir

# only some sub cmd needs dist file
flags.DEFINE_string(
    'conf_file', '', 'Cluster config file, supports two styles: yaml and hosts. ',
    short_name='f')

def check_version(version_map : dict):
    f_version = ''
    f_endpoint = ''
    f_role = ''
    flag = True
    for k, v in version_map.items():
        for endpoint, cur_version in v:
            if f_version == '':
                f_version = cur_version
                f_endpoint = endpoint
                f_role = k
            if cur_version != f_version:
                logging.warn(f'version mismatch. {k} {endpoint} version {cur_version}, {f_role} {f_endpoint} version {f_version}')
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
                if yaml_conf_dict['mode'] == 'cluster' and role == 'taskmanager':
                    taskmanager_validator = TaskManagerConfValidator(cur_conf)
                    if not taskmanager_validator.validate():
                        logging.warn(f'taskmanager {endpoint} conf check failed')
                        flag = False

    if yaml_conf_dict['mode'] == 'standalone':
        conf_validator = StandaloneConfValidator(detail_conf_map['nameserver'][0], detail_conf_map['tablet'][0])
    else:
        conf_validator = ClusterConfValidator(yaml_conf_dict, detail_conf_map)
    if conf_validator.validate() and flag:
        logging.info('check conf ok')
    else:
        logging.warn('check conf failed')

def check_log(yaml_conf_dict, log_map):
    flag = True
    for role, v in log_map.items():
        for endpoint, values in v.items():
            log_analysis = LogAnalysis(role, endpoint, values)
            if not log_analysis.analysis_log() : flag = False
    if flag:
        logging.info('check logging ok')

def main(argv):
    conf_opt = ConfOption()
    if not conf_opt.init():
        return
    util.clean_dir(conf_opt.data_dir)
    dist_conf = YamlConfReader(conf_opt.dist_conf).conf()
    yaml_validator = YamlConfValidator(dist_conf.full_conf)
    if not yaml_validator.validate():
        logging.warning("check yaml conf failed")
        sys.exit()
    logging.info("check yaml conf ok")

    logging.info("mode is {}".format(dist_conf.mode))
    if dist_conf.mode == 'cluster' and conf_opt.env != 'onebox':
        collector = Collector(dist_conf)
        if conf_opt.check_version():
            version_map = collector.collect_version()
        if conf_opt.check_conf():
            collector.pull_config_files(f'{conf_opt.data_dir}/conf')
        if conf_opt.check_log():
            collector.pull_log_files(f'{conf_opt.data_dir}/logging')
        if conf_opt.check_conf() or conf_opt.check_log():
            file_map = util.get_files(conf_opt.data_dir)
            logging.debug("file_map: %s", file_map)
    else:
        collector = LocalCollector(dist_conf)
        if conf_opt.check_version():
            version_map = collector.collect_version()
        if conf_opt.check_conf() or conf_opt.check_log():
            file_map = collector.collect_files()
            logging.debug("file_map: %s", file_map)

    if conf_opt.check_version():
        flag, version = check_version(version_map)
        if flag:
            logging.info(f'openmldb version is {version}')
            logging.info('check version ok')
        else:
            logging.warn('check version failed')

    if conf_opt.check_conf():
        check_conf(dist_conf.full_conf, file_map['conf'])
    if conf_opt.check_log():
        check_log(dist_conf.full_conf, file_map['logging'])

def status(args):
    """use OpenMLDB Python SDK to connect OpenMLDB"""
    conn = Connector()
    status_checker = checker.StatusChecker(conn)
    assert status_checker.check_components()

    # --diff with dist conf file, conf_file is required
    if args.diff:
        assert flags.FLAGS.conf_file
        status_checker.check_startup('') # TODO

def inspect(args):
    insepct_online(args)
    inspect_offline(args)

def insepct_online(args):
    """insert is not a good idea? show table status?"""
    # TODO(hw)

def inspect_offline(args):
    """"""
    assert checker.StatusChecker(Connector()).offline_support()


def test_sql(args):
    conn = Connector()
    status_checker = checker.StatusChecker(conn)
    if not status_checker.check_components():
        logging.warning("some server is offline, be careful")
    tester = checker.SQLTester(conn)
    tester.setup()
    tester.online()
    if status_checker.offline_support():
        tester.offline()
    else:
        print("no taskmanager, can't test offline")
    tester.teardown()

def main1(argv):
    parser = argparse_flags.ArgumentParser()
    # use args.header returned by parser.parse_args
    subparsers = parser.add_subparsers(help='The command to execute.')

    status_parser = subparsers.add_parser(
        'status', help='check the OpenMLDB server status')
    status_parser.add_argument('--diff', default=False, type=lambda x: (str(x).lower() == 'true'), 
        help='check if all endpoints in conf are in cluster, true/false. If true, need to set `--conf_file`')
    status_parser.set_defaults(command=status)
    test_parser = subparsers.add_parser('test', help='do simple create&insert test, what about offline?')
    test_parser.set_defaults(command=test_sql)
    inspect_parser = subparsers.add_parser(
        'inspect', help='Inspect online and offline. Use `inspect [online/offline]` to inspect one. Support table status later')
    inspect_parser.set_defaults(command=inspect)
    inspect_sub = inspect_parser.add_subparsers()
    online = inspect_sub.add_parser('online', help='only inspect online table')
    online.set_defaults(command=insepct_online)
    offline = inspect_sub.add_parser('offline', help='only inspect offline jobs, check the job log')
    offline.set_defaults(command=inspect_offline)
    args = parser.parse_args(argv)

    args.command(args)


def run():
    app.run(main1)

if __name__ == '__main__':
    app.run(main1)
