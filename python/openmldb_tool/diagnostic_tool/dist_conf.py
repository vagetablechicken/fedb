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
from absl import logging
from absl import flags
import configparser as cfg
import yaml
from . import util

ALL_SERVER_ROLES = ["nameserver", "tablet", "apiserver", "taskmanager"]

CXX_SERVER_ROLES = ALL_SERVER_ROLES[:3]

JAVA_SERVER_ROLES = [ALL_SERVER_ROLES[3]]

flags.DEFINE_string("default_dir", "/work/openmldb", "OPENMLDB_HOME")


class ServerInfo:
    def __init__(self, role, endpoint, path, is_local):
        self.role = role
        self.endpoint = endpoint
        self.path = path
        self.host = endpoint.split(":")[0]
        self.is_local = is_local

    def __str__(self):
        return f"Server[{self.role}, {self.endpoint}, {self.path}]"

    def is_taskmanager(self):
        return self.role == "taskmanager"

    def conf_path(self):
        return f"{self.path}/conf"

    def bin_path(self):
        return f"{self.path}/bin"

    def taskmanager_path(self):
        return f"{self.path}/taskmanager"

    def conf_path_pair(self, local_root):
        config_name = (
            f"{self.role}.flags"
            if self.role != "taskmanager"
            else f"{self.role}.properties"
        )
        local_prefix = f"{self.endpoint}-{self.role}"
        return (
            f"{self.path}/conf/{config_name}",
            f"{local_root}/{local_prefix}/{config_name}",
        )

    def remote_log4j_path(self):
        return f"{self.path}/taskmanager/conf/log4j.properties"

    # TODO(hw): openmldb glog config? will it get a too large log file? fix the settings
    def remote_local_pairs(self, remote_dir, file, dest):
        return f"{remote_dir}/{file}", f"{dest}/{self.endpoint}-{self.role}/{file}"

    def cmd_on_host(self, cmd):
        if self.is_local:
            return util.local_cmd(cmd)
        else:
            _, stdout, _ = util.SSH().exec(self.host, cmd)
            return util.buf2str(stdout)


class ServerInfoMap:
    def __init__(self, server_info_map):
        # map struct: <role,[server_list]>
        self.map = server_info_map

    def items(self):
        return self.map.items()

    def for_each(self, func, roles=None, check_result=True):
        """
        even some failed, call func for all
        :param roles:
        :param func:
        :param check_result:
        :return:
        """
        if roles is None:
            roles = ALL_SERVER_ROLES
        ok = True
        for role in roles:
            if role not in self.map:
                logging.warning("role %s is not in map", role)
                ok = False
                continue
            for server_info in self.map[role]:
                res = func(server_info)
                if check_result and not res:
                    ok = False
        return ok


class DistConf:
    def __init__(self, conf_dict: dict):
        self.full_conf = conf_dict
        self.mode = self.full_conf["mode"]
        self.server_info_map = ServerInfoMap(
            self._map(
                ALL_SERVER_ROLES + ["zookeeper"],
                lambda role, s: ServerInfo(
                    role,
                    s["endpoint"],
                    s["path"] if "path" in s and s["path"] else flags.FLAGS.default_dir,
                    flags.FLAGS.local or (s["is_local"] if "is_local" in s else False),
                ),
            )
        )

        # if "zookeeper":
        # endpoint = server.endpoint.split('/')[0]
        # # host:port:zk_peer_port:zk_election_port
        # endpoint = ':'.join(endpoint.split(':')[:2])

    def is_cluster(self):
        return self.mode == "cluster"

    def __str__(self):
        return str(self.full_conf)

    def _map(self, role_list, trans):
        result = {}
        for role in role_list:
            if role not in self.full_conf:
                continue
            ss = self.full_conf[role]
            if ss:
                result[role] = []
                for s in ss:
                    result[role].append(trans(role, s) if trans is not None else s)
        return result

    def count_dict(self):
        d = {r: len(s) for r, s in self.server_info_map.items()}
        assert not self.is_cluster() or d["zookeeper"] >= 1
        return d


class YamlConfReader:
    def __init__(self, config_path):
        with open(config_path, "r") as stream:
            self.dist_conf = DistConf(yaml.safe_load(stream))

    def conf(self):
        return self.dist_conf


class HostsConfReader:
    def __init__(self, config_path):
        with open(config_path, "r") as stream:
            # hosts style to dict
            cf = cfg.ConfigParser(strict=False, delimiters=" ", allow_no_value=True)
            cf.read_file(stream)
            d = {}
            for sec in cf.sections():
                # k is endpoint, v is path or empty, multi kv means multi servers
                d[sec] = [{"endpoint": k, "path": v} for k, v in cf[sec].items()]

            d["mode"] = "cluster"
            self.dist_conf = DistConf(d)

    def conf(self):
        return self.dist_conf


class ConfParser:
    def __init__(self, config_path):
        self.conf_map = {}
        with open(config_path, "r") as stream:
            for line in stream:
                item = line.strip()
                if item == "" or item.startswith("#"):
                    continue
                arr = item.split("=")
                if len(arr) != 2:
                    continue
                if arr[0].startswith("--"):
                    # for gflag
                    self.conf_map[arr[0][2:]] = arr[1]
                else:
                    self.conf_map[arr[0]] = arr[1]

    def conf(self):
        return self.conf_map


def read_conf(conf_file):
    """if not yaml style, hosts style"""
    try:
        conf = YamlConfReader(conf_file).conf()
    except Exception as e:
        logging.debug(f"yaml read failed on {e}, read in hosts style")
        conf = HostsConfReader(conf_file).conf()
    return conf
