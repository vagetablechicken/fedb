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

import os
import pytest
from diagnostic_tool.conf_validator import ConfValidator
from diagnostic_tool.dist_conf import read_conf

from absl import flags

def test_validate_dist_conf():
    flags.FLAGS['local'].parse('False')
    dist = read_conf(os.path.dirname(__file__) + "/hosts")
    print(dist)
    assert ConfValidator(dist).validate()
    # some servers in hosts don't have field `path``
    with pytest.raises(AssertionError):
        assert ConfValidator(dist).validate(require_dir=True)
    
    dist = read_conf(os.path.dirname(__file__) + "/cluster_dist.yml")
    print(dist)
    # zk has no path but we don't check it
    assert ConfValidator(dist).validate(require_dir=True)
