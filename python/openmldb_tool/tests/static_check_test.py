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
import os.path
import pytest

from diagnostic_tool.collector import Collector
from diagnostic_tool.conf_validator import ClusterConfValidator
from diagnostic_tool.dist_conf import read_conf
from absl import flags

logging.basicConfig(
    level=logging.DEBUG,
    format="{%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)

current_path = os.path.dirname(__file__)

def mock_path(dist_conf):
    for k,v in dist_conf.server_info_map.items():
        for server in v:
            if server.path:
                server.path = current_path + '/sbin_test' + server.path


# Remote test require ssh config, skip now. Only test local collector
def test_local_collector():
    flags.FLAGS['local'].parse('True') # only test local
    flags.FLAGS['default_dir'].parse('/work/openmldb')
    dist_conf = read_conf(current_path + "/hosts")
    mock_path(dist_conf)
    local_collector = Collector(dist_conf)
    # # no need to ping localhost when flags.FLAGS.local==True
    # with pytest.raises(AssertionError):
    #     local_collector.ping_all()

    # # no bin in tests/sbin_test/, so it's a empty map
    # version_map = local_collector.collect_version()
    # assert not version_map

    assert local_collector.pull_config_files("/tmp/conf_copy_dest")
    assert dist_conf.is_cluster()
    assert ClusterConfValidator(dist_conf, "/tmp/conf_copy_dest").validate()

    # all no logs
    # assert not local_collector.pull_log_files("/tmp/log_copy_dest")
#     def test_pull_logs(self):
#         # no logs in tablet1
#         with self.assertLogs() as cm:
#             self.assertFalse(self.conns.pull_log_files("/tmp/log_copy_to"))
#         for log_str in cm.output:
#             logging.info(log_str)
#         self.assertTrue(any(["no file in" in log_str for log_str in cm.output]))

#     @pytest.skip
#     @patch("diagnostic_tool.collector.parse_config_from_properties")
#     def test_version(self, mock_conf):
#         mock_conf.return_value = os.path.dirname(__file__) + "/work/spark_home"
#         self.assertTrue(self.conns.collect_version())
