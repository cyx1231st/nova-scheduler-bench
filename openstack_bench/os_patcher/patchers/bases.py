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


import abc
import traceback

from oslo_log import log as logging

from openstack_bench import interceptions
from openstack_bench.os_patcher import patching as bench_patching


LOG = logging.getLogger(__name__)
_UNDFINED = "UNDEFINED"


def printerror(error_str):
    print(error_str)


class BasePatcher(object):
    """ BasePatcher for OpenStack projects """

    __metaclass__ = abc.ABCMeta

    REPOSITORY = _UNDFINED
    PATCH_POINT = _UNDFINED
    CONF = _UNDFINED

    def __init__(self, service_name, host_name, release=None):
        self.release = release
        self.service_name = service_name
        self.host_name = host_name
        self.log_prefix = "BENCH-" + service_name + "-" + host_name + ": "

        self.skipped = ""
        self.patched = ""
        self.failed = ""

    def stub_entrypoint(self, patch_func):
        if self.PATCH_POINT == _UNDFINED:
            raise RuntimeError("Undefined patch entrypoint!")
        bench_patching.AopPatch.logger = staticmethod(printerror)
        bench_patching.AopPatch(
            self.PATCH_POINT,
            after=lambda arg: patch_func(),
            direct=True)

    @abc.abstractmethod
    def stub_out_modules(self):
        """ Stub out repository modules """

    @abc.abstractmethod
    def _override_configurations(self):
        """ Stub out repository configurations """

    def override_configurations(self, is_console, is_debug, folder):
        """ Stub out repository configurations """
        self.conf("host", self.host_name)

        # Enable console mode
        if not is_console:
            self.conf(
                "logging_default_format_string",
                "%(asctime)s.%(msecs)03d %(levelname)s %(name)s [-] "
                "%(instance)s%(message)s")
            self.conf(
                "logging_debug_format_suffix",
                "from (pid=%(process)d) %(funcName)s "
                "%(pathname)s:%(lineno)d")
            self.conf(
                "logging_exception_prefix",
                "%(asctime)s.%(msecs)03d TRACE %(name)s %(instance)s")
            self.conf(
                "logging_context_format_string",
                "%(asctime)s.%(msecs)03d %(levelname)s %(name)s "
                "[%(request_id)s %(user_name)s %(project_name)s] "
                "%(instance)s%(message)s")
            self.conf(
                "log_file",
                folder + "BENCH-" + self.service_name + "-"
                + self.host_name + ".log")

        # Enable debug mode
        self.conf("debug", is_debug)

        # Custom overriding
        self._override_configurations()

    def inject_logs(self, points, engine):
        bench_patching.AopPatch.logger = staticmethod(self.error)
        bench_patching.AopPatch.printer = staticmethod(self.printer)

        i_point = interceptions.ReleasePoint(
            after=lambda arg:
                "Bench initiated %s %s!\n"
                "Errors:\n%s\n"
                "Patched:\n%s"
                "Skipped:\n%s"
                "Failed:\n%s" % (
                    self.release,
                    engine.subvirted,
                    engine.errors,
                    self.patched,
                    self.skipped,
                    self.failed))
        point = interceptions.AnalysisPoint("oslo_log.log.setup",
                                            project=self.REPOSITORY)
        point[None] = i_point

        _points = points[:]
        _points.append(point)

        for point in _points:
            if point.project != self.REPOSITORY:
                raise RuntimeError("Project don't match: %s, %s"
                                   % (point.project, self.REPOSITORY))

            if self.release in point:
                i_point = point[self.release]
            elif None in point:
                i_point = point[None]
            else:
                print("Skip point %s" % point.inject_point)
                self.skipped += "   %s\n" % point.inject_point
                continue
            try:
                bench_patching.AopPatch(point.inject_point,
                                        before=i_point.before,
                                        after=i_point.after,
                                        excep=i_point.excep)
                print("Load point %s" % point.inject_point)
                self.patched += "   %s\n" % point.inject_point
            except Exception:
                e_stack = traceback.format_exc()
                print("Failed to load %s!" % point.inject_point)
                print("Traceback:\n%s" % e_stack)
                self.failed += "   %s\n" % point.inject_point

    # helper methods
    def patch(self, name, attr, add=False):
        """ Patch module name with attr """
        bench_patching.MonkeyPatch(name, attr, add=add)

    def conf(self, name, val, group=None):
        self.CONF.set_override(name, val, group)

    # loggers
    def printer(self, msg):
        LOG.warn(self.log_prefix + msg)

    def error(self, msg):
        LOG.error(self.log_prefix + msg)
