# Copyright (c) 2016 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
from os import path

from oslo_config import cfg

from openstack_bench import bench_drivers


_file_path = path.dirname(os.path.realpath(__file__))
_DIR = path.join(_file_path, "../bench.conf")

CONF_BENCH = cfg.ConfigOpts()


_default_opts = [
    cfg.StrOpt("bench_driver",
               choices=bench_drivers.get_driver_names(),
               default=bench_drivers.DEFAULT_DRIVER),
]


CONF_BENCH.register_opts(_default_opts)
CONF_BENCH(['--config-file', _DIR], project="bench")
