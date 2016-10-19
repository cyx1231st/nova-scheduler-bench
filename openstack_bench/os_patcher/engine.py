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

import traceback

from openstack_bench import bench_drivers
from patchers import load_patcher


def printerror(error_str):
    print(error_str)


class PatchEngine(object):
    def __init__(self, args):
        self.args = args

        self.patcher = load_patcher(args.service, args.host)

        self.driver_obj = bench_drivers.from_config()
        self.driver_obj.release = self.patcher.release
        if not self.driver_obj.check_service(args.service):
            raise RuntimeError("Driver doesn't support service %s!"
                               % args.service)

        self.subvirted = False
        self.errors = "   None"

    def _apply_patch(self):
        if self.subvirted:
            self.errors = "Already subvirted!"
            return
        try:
            self.patcher.printer("Patching...")

            # Patch repository specific modules
            self.patcher.stub_out_modules()

            # Patch repository specific configurations
            self.patcher.override_configurations(self.args.console,
                                                 self.args.debug,
                                                 self.args.result_folder)

            # Inject logs dynamically
            points = self.driver_obj.points.values()
            self.patcher.inject_logs(points, self)

            self.patcher.printer("Patching Success!")
            self.subvirted = True
        except Exception:
            self.errors = traceback.format_exc()
            print("Engine failed:\n%s" % self.errors)

    def subvirt(self):
        self.patcher.stub_entrypoint(self._apply_patch)
        self.patcher.run_service()
