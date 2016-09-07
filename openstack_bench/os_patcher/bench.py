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
import engine
from openstack_bench.config import CONF_BENCH
from openstack_bench import bench_drivers


class BenchmarkMeta(object):
    DEFAULT_HOST = "_local_"

    def __init__(self, args):
        self.host = args.host
        self.is_debug = args.debug
        self.is_console = args.console
        self.service = args.service
        self.log_prefix = "BENCH-" + self.service + "-" + self.host + ": "
        self.folder = args.result_folder + "/"


def init(args):
    meta = BenchmarkMeta(args)

    driver_class_name = CONF_BENCH.bench_driver
    driver_obj = bench_drivers.init_driver(driver_class_name, meta)

    patch_engine = engine.PatchEngine(meta, driver_obj)
    patch_engine.subvirt(args.service)
