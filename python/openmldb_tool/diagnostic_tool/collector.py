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
import logging
import os
import re

from diagnostic_tool.dist_conf import (
    DistConf,
    CXX_SERVER_ROLES,
    ServerInfo,
    JAVA_SERVER_ROLES,
    ConfParser,
)
import diagnostic_tool.util as util
from .util import SSH
from absl import flags

log = logging.getLogger(__name__)


def parse_config_from_properties(props_str, config_name) -> str:
    f"""
    the config line must start from {config_name}, no comment
    :param props_str:
    :param config_name:
    :return:
    """
    config_name = re.escape(config_name)
    m = re.search(
        rf"^{config_name}.*",
        props_str,
    )
    if not m:
        return ""
    conf_line = m.group(0)  # the whole line
    # TODO(hw): what if relative path
    return conf_line.split("=")[1]


class Collector:
    """
    For each server, if is_local, run cmd in localhost, else use ssh
    We should get job logs by `SHOW JOBLOG`
    """

    def __init__(self, dist_conf: DistConf):
        self.dist_conf = dist_conf

    def ping_all(self) -> bool:
        """
        test ssh
        return False if command got some errors
        throw SSHException if the server fails to execute the command
        :return: bool
        """
        assert not flags.FLAGS.local, "local servers don't need to test ping"

        def ping(server_info: ServerInfo) -> bool:
            res = server_info.cmd_on_host("whoami && pwd")
            log.debug(res)
            if not res:
                log.warning(f"failed to ping {server_info}")
                return False
            return True

        return self.dist_conf.server_info_map.for_each(ping)

    def pull_config_files(self, dest) -> bool:
        def pull_one(server_info: ServerInfo) -> bool:
            # if taskmanager, pull taskmanager.properties, no log4j
            config_paths = server_info.conf_path_pair(dest)
            log.debug(f"pull config file from {server_info}: {config_paths}")
            if server_info.is_local:
                return self.copy_local_file(config_paths)
            return self.pull_file(server_info.host, config_paths)

        return self.dist_conf.server_info_map.for_each(pull_one)

    def pull_log_files(self, dest) -> bool:
        def pull_log(server_info: ServerInfo) -> bool:
            """
            After 0.7.0, onebox cluster started by sbin won't use the same root dir
            No need to check tablet2.flags
            """
            # get log path
            def get_config_value(server_info: ServerInfo, conf_path, config_name, default_v):
                log.debug("get %s from %s", config_name, conf_path)
                grep_str = server_info.cmd_on_host(f"grep {config_name} {conf_path}")
                tmp = parse_config_from_properties(grep_str, config_name)
                return tmp if tmp else default_v

            if server_info.is_taskmanager():
                log4j_conf_path = server_info.remote_log4j_path()
                log_path = get_config_value(
                    server_info, log4j_conf_path, "log4j.appender.file.file=", "./logs/taskmanager.log"
                )
            else:
                conf_path = server_info.conf_path_pair("")[0]
                log_path = get_config_value(server_info, conf_path, "openmldb_log_dir=", "./logs")
            log_path_pair = server_info.remote_local_pairs(server_info.path, log_path, dest)
            log.debug(f"cp pair: {log_path_pair}")
            # def serverinfo.smart_cp()
            
            # if server_info.is_local:
            #     #cp
            #     log.debug(f"get from local {server_info.host}")
            #     return self.copy_local_file(config_paths)
            # else:
            #     #scp
            return True

        return self.dist_conf.server_info_map.for_each(pull_log)
        

    def collect_version(self):
        """
        get the version of components before starts
        :return:
        """
        version_map = {}

        def extract_version(raw_version):
            return raw_version.split(" ")[2].split("-")[0]

        def extract_java_version(raw_version):
            arr = raw_version.split("-")
            if len(arr) < 2:
                return ""
            return arr[0]

        def run_version(server_info: ServerInfo) -> bool:
            version_map.setdefault(server_info.role, [])
            version = server_info.cmd_on_host(
                f"{server_info.path}/bin/openmldb --version"
            )
            if not version:
                log.warning("failed at get version from %s", server_info)
            else:
                version_map[server_info.role].append(
                    (server_info.endpoint, extract_version(version))
                )
            return True

        self.dist_conf.server_info_map.for_each(run_version, CXX_SERVER_ROLES)

        def jar_version(server_info: ServerInfo) -> bool:
            def get_spark_home(server_info: ServerInfo):
                """
                https://openmldb.ai/docs/zh/main/deploy/install_deploy.html#taskmanager use env, but won't store it
                :param remote_config_file:
                :return: abs path
                """
                tm_conf_path = server_info.conf_path_pair("")[0]
                config_name = "spark.home="
                log.debug("get %s from %s", config_name, tm_conf_path)
                grep_str = server_info.cmd_on_host(f"grep {config_name} {tm_conf_path}")

                if not grep_str:
                    # TODO(hw):no config in file, get env SPARK_HOME?
                    #  what if ssh user is different with server user or it's a temp env?
                    # or force to set spark home in config?
                    # _, stdout, _ = self.ssh_client.exec_command(f'env | grep SPARK_HOME')
                    # env = stdout.read()
                    # if not env:
                    #     raise RuntimeError('no env SPARK_HOME')
                    return ""

                # may have spark home in config(discard if it's in comment)
                return parse_config_from_properties(grep_str, config_name)

            def get_batch_version(server_info: ServerInfo):
                # TODO(hw): check if multi batch jars
                spark_home = get_spark_home(server_info)
                log.debug("spark_home %s", spark_home)
                if not spark_home:
                    spark_home = server_info.path + '/spark'
                    log.debug(f"try local spark in server deploy path: {spark_home}")
                batch_jar_path = f"{spark_home}/jars/openmldb-batch-*"
                return server_info.cmd_on_host(
                    f"java -cp {batch_jar_path} com._4paradigm.openmldb.batch.utils.VersionCli"
                ).strip()

            bv = get_batch_version(server_info)
            if bv:
                version = extract_java_version(bv)
                if version != "":
                    version_map.setdefault("openmldb-batch", [])
                    version_map["openmldb-batch"].append((server_info.host, version))
                else:
                    log.warning(f"{bv}")
            else:
                log.warning("failed at get batch version from %s", server_info)

            def get_taskmanager_version(server_info: ServerInfo):
                tm_root_path = server_info.taskmanager_path()
                # TODO(hw): check if multi taskmanager jars
                return server_info.cmd_on_host(
                    f"java -cp {tm_root_path}/lib/openmldb-taskmanager-* com._4paradigm.openmldb.taskmanager.utils.VersionCli",
                ).strip()

            tv = get_taskmanager_version(server_info)
            if tv:
                version = extract_java_version(tv)
                if version != "":
                    version_map.setdefault("taskmanager", [])
                    version_map["taskmanager"].append((server_info.host, version))
                else:
                    log.warning(f"{tv}")
            else:
                log.warning("failed at get taskmanager version from %s", server_info)
            return True

        self.dist_conf.server_info_map.for_each(jar_version, JAVA_SERVER_ROLES)
        return version_map

    def pull_cxx_server_logs(self, server_info, dest, last_n) -> bool:
        """
        nameserver, tablet: config name openmldb_log_dir
        :param server_info:
        :param dest:
        :param last_n:
        :return:
        """
        def get_config_value(server_info: ServerInfo):
            conf_path = server_info.conf_path_pair("")[0]

        server_log_dir = get_config_value(
            server_info, remote_conf_path, "openmldb_log_dir=", "./logs"
        )
        # TODO(hw): what if `openmldb_log_dir` is abs path
        server_log_dir = f"{server_info.path}/{server_log_dir}"
        # only get info log, no soft link file
        log_list = self.get_log_files(server_info, server_log_dir)
        log_list = self.filter_file_list(
            log_list,
            lambda di: f"{server_info.role}.info.log" in di["filename"],
            last_n,
        )
        return self.pull_files(server_info, server_log_dir, log_list, dest)

    def pull_tm_server_logs(self, server_info, dest, last_n) -> bool:
        """
        taskmanager: config name log4j.appender.file.file= in log4j, start from taskmanager/bin/
        :param server_info:
        :param dest:
        :param last_n:
        :return:
        """
        # job log path is in config
        if not server_info.is_taskmanager():
            return False
        remote_conf_path = server_info.remote_log4j_path()
        server_log_file_pattern = self.get_config_value(
            server_info, remote_conf_path, "log4j.appender.file" ".file=", ""
        )
        # file.file is a file name, not a dir
        server_log_dir = os.path.split(server_log_file_pattern)[0]

        # TODO(hw): what if abs path?
        # dir is start from taskmanager/bin
        server_log_dir = f"{server_info.taskmanager_path()}/bin/{server_log_dir}"

        log_list = self.get_log_files(server_info, server_log_dir)
        log_list = self.filter_file_list(
            log_list, lambda di: "taskmanager.log" in di["filename"], last_n
        )
        return self.pull_files(server_info, server_log_dir, log_list, dest)

    def copy_local_file(self, paths) -> bool:
        src_path, local_path = paths[0], paths[1]
        try:
            # ensure local path is exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            os.system(f"cp {src_path} {local_path}")
        except Exception as e:
            log.warning(f"local copy {src_path}:{local_path} error on , err: {e}")
            return False
        return True

    def pull_file(self, remote_host, paths) -> bool:
        remote_path, local_path = paths[0], paths[1]
        log.debug(f"remote {remote_path}, local: {local_path}")
        SSH().connect(hostname=remote_host)
        sftp = SSH().open_sftp()
        try:
            # ensure local path is exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            # local path must be a file, not a dir
            sftp.get(remote_path, local_path)
        except Exception as e:
            log.warning(
                f"pull from remote {remote_host}:{remote_path} error on , err: {e}"
            )
            return False
        return True

    def pull_files(self, server_info, remote_path, file_list, dest) -> bool:
        if not file_list:
            log.warning("no file in %s on %s", remote_path, server_info)
            return False
        if server_info.is_local:
            return all(
                [
                    self.copy_local_file(
                        server_info.remote_local_pairs(remote_path, file, dest)
                    )
                    for file in file_list
                ]
            )
        else:
            return all(
                [
                    self.pull_file(
                        server_info.host,
                        server_info.remote_local_pairs(remote_path, file, dest),
                    )
                    for file in file_list
                ]
            )

    def get_log_dir_from_conf(self, remote_config_file, server_info):
        """
        nameserver, tablet: server logs
        taskmanager: config file only has job log path, get taskmanager log by log4j

        :param remote_config_file:
        :param server_info:
        :return:
        """
        config_name = "openmldb_log_dir"
        default_dir = "/logs"
        if server_info.role == "taskmanager":
            # log4j logs, not the job logs
            config_name = "job.log.path"
            # taskmanager '../log' is from 'bin/', so it's '/log'.
            # TODO(hw): fix taskmanager start dir
            default_dir = "/log"

        log.debug("get %s from %s", config_name, remote_config_file)
        _, stdout, _ = self.ssh_client.exec_command(
            f"grep {config_name} {remote_config_file}"
        )
        grep_str = buf2str(stdout)

        if not grep_str:
            return server_info.path + default_dir
        # may set log dir path in config
        return parse_config_from_properties(grep_str, config_name)

    def get_log_files(self, server_info, log_dir):
        if server_info.is_local:
            log_dir = os.path.normpath(log_dir)
            log.debug("get logs from %s", log_dir)
            # if no the log dir, let it crash
            logs = []
            for name in os.listdir(log_dir):
                stat = os.stat(os.path.join(log_dir, name))
                logs.append({"filename": name, "st_mtime": stat.st_mtime})
        else:
            host = server_info.host
            self.ssh_client.connect(hostname=host)
            sftp = self.ssh_client.open_sftp()

            log_dir = os.path.normpath(log_dir)
            log.debug("get logs name from %s, %s", log_dir, host)
            # if no the log dir, let it crash
            logs = [attr.__dict__ for attr in sftp.listdir_attr(log_dir)]
        return logs

    def filter_file_list(self, logs, filter_func, last_n):
        logs = list(filter(filter_func, logs))

        # avoid soft link file?
        # sort by modify time
        logs.sort(key=lambda x: x["st_mtime"], reverse=True)
        log.debug("all_logs(sorted): %s", logs)
        # get last n
        logs = [log_attr["filename"] for log_attr in logs[:last_n]]
        log.debug("get last %d: %s", last_n, logs)
        return logs


class LocalCollector:
    def __init__(self, dist_conf: DistConf):
        self.dist_conf = dist_conf

    def get_tablet_conf_file(self, conf_path, endpoint):
        conf_file = "tablet.flags"
        full_path = os.path.join(conf_path, conf_file)
        detail_conf = ConfParser(full_path).conf()
        if detail_conf["endpoint"] == endpoint:
            return conf_file
        else:
            return "tablet2.flags"

    def get_taskmanager_logs(self, root_path, last_n):
        taskmanager_logs = util.get_local_logs(root_path, "taskmanager")
        names = os.listdir(root_path)
        job_logs = []
        for file_name in names:
            if file_name.startswith("job") and file_name.endswith("error.log"):
                stat = os.stat(os.path.join(root_path, file_name))
                job_logs.append({"filename": file_name, "st_mtime": stat.st_mtime})
        job_logs.sort(key=lambda x: x["st_mtime"], reverse=True)
        job_logs = [log_attr["filename"] for log_attr in job_logs[:last_n]]
        job_logs = [
            (file_name, os.path.join(root_path, file_name)) for file_name in job_logs
        ]
        return job_logs

    def collect_files(self):
        file_map = {"conf": {}, "log": {}}
        for role, value in self.dist_conf.server_info_map.map.items():
            file_map["conf"][role] = {}
            file_map["log"][role] = {}
            for item in value:
                file_map["conf"][role].setdefault(item.endpoint, [])
                if self.dist_conf.mode == "cluster":
                    if role == "taskmanager":
                        conf_file = f"taskmanager.properties"
                    elif role == "tablet":
                        conf_file = self.get_tablet_conf_file(
                            item.conf_path(), item.endpoint
                        )
                    else:
                        conf_file = f"{role}.flags"
                else:
                    conf_file = f"standalone_{role}.flags"
                full_path = os.path.join(item.conf_path(), conf_file)
                file_map["conf"][role][item.endpoint].append((conf_file, full_path))
                detail_conf = ConfParser(full_path).conf()
                if role == "taskmanager":
                    log_dir = (
                        detail_conf["job.log.path"]
                        if "job.log.path" in detail_conf
                        else "./logs"
                    )
                    item.path = item.path + "/taskmanager/bin"
                else:
                    log_dir = (
                        detail_conf["openmldb_log_dir"]
                        if "openmldb_log_dir" in detail_conf
                        else "./logs"
                    )
                full_log_dir = (
                    log_dir
                    if log_dir.startswith("/")
                    else os.path.join(item.path, log_dir)
                )
                if role == "taskmanager":
                    file_map["log"][role][item.endpoint] = self.get_taskmanager_logs(
                        full_log_dir, 2
                    )
                else:
                    file_map["log"][role][item.endpoint] = util.get_local_logs(
                        full_log_dir, role
                    )
        return file_map
