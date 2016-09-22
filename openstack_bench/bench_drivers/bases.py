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

from oslo_log import log as logging

import openstack_bench.os_patcher.patching as bench_patching


LOG = logging.getLogger(__name__)


class AnalysisPoint(object):
    BEFORE = object()
    AFTER = object()
    EXCEPT = object()

    def __init__(self,
                 inject_point,
                 inject_place,
                 f_build_msg,
                 releases=None,
                 except_type=None):
        self.inject_point = inject_point
        self.inject_place = inject_place
        self.f_build_msg = f_build_msg
        self.releases = releases
        self.except_type = except_type


class BenchDriverBase(object):
    def __init__(self, meta):
        self.meta = meta

    def _inject_logs(self):
        raise NotImplementedError()

    def inject_logs(self):
        bench_patching.AopPatch.logger = self.error
        self.patch_aop(
            "oslo_log.log.setup",
            after=lambda *args, **kwargs:
                self.warn("Bench initiated!"))
        self._inject_logs()

    # patchings
    def patch_aop(self, name, before=None, after=None):
        bench_patching.AopPatch(name, before=before, after=after)

    # loggers
    def info(self, msg):
        LOG.info(self.meta.log_prefix + msg)

    def debug(self, msg):
        LOG.debug(self.meta.log_prefix + msg)

    def warn(self, msg):
        LOG.warn(self.meta.log_prefix + msg)

    def error(self, msg):
        LOG.error(self.meta.log_prefix + msg)
