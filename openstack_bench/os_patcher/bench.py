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

import sys

from nova.cmd import api
from nova.cmd import compute
from nova.cmd import conductor
from nova.cmd import scheduler

import patching as bench_patching


BENCH = None


class BenchmarkMeta(object):
    DEFAULT_HOST = "_local_"
    DEFAULT_DRIVER = "driver_scheduler"

    def __init__(self, args):
        self.enabled = True
        self.host = args.host
        self.is_debug = args.debug
        self.is_console = args.console
        self.service = args.service
        self.log_prefix = "BENCH-" + self.service + "-" + self.host + ": "
        self.folder = args.result_folder + "/"
        self.v_type = args.view
        self.scheduler_type = args.scheduler_type
        """
        driver_name = args.driver
        try:
            module = __import__(driver_name, globals(), level=1)
            self.driver = module.get_driver(self)
            if not isinstance(self.driver, BenchDriverBase):
                raise RuntimeError()
        except Exception:
            self.enabled = False
            raise
        """
        self.release = args.release.lower()
        from openstack_bench.bench_drivers import driver_scheduler
        self.driver = driver_scheduler.get_driver(self)


def printerror(error_str):
    print(error_str)


def init(args):
    bench_patching.AopPatch.logger = printerror
    bench_patching.AopPatch(
        "nova.config.parse_args",
        after=lambda *args, **kwargs: patch_nova())

    global BENCH
    BENCH = BenchmarkMeta(args)

    sys.argv = [""]
    sys.argv.append("--config-file")
    sys.argv.append("/etc/nova/nova.conf")
    service = args.service
    if service == "compute":
        sys.argv[0] = "nova-compute"
        compute.main()
    elif service == "conductor":
        sys.argv[0] = "nova-conductor"
        conductor.main()
    elif service == "scheduler":
        sys.argv[0] = "nova-scheduler"
        scheduler.main()
    elif service == "api":
        sys.argv[0] = "nova-api"
        api.main()
    else:
        print "Unsupported service %s" % service


def patch_nova():
    if not BENCH or not BENCH.enabled:
        return

    BENCH.driver.stubout_nova()
    BENCH.driver.stubout_conf()
    BENCH.driver.inject_logs()
