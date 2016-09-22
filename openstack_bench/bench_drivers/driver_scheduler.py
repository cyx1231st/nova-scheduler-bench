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

from openstack_bench.bench_drivers import register_driver
from openstack_bench.config import CONF_BENCH
from openstack_bench.releases import Release

import bases
from bases import AnalysisPoint


def debug(*args, **kwargs):
    import pdb
    pdb.set_trace()


class BenchDriverScheduler(bases.BenchDriverBase):
    def __init__(self, meta, host_manager=None):
        # TODO: handle releases in nova_patcher
        self.release = Release[CONF_BENCH.nova_patcher.release]
        super(BenchDriverScheduler, self).__init__(meta)

    def register_points(self):
        f_ins_name = lambda arg: arg['body']['server']['name']

        self.register_point(
            "nova.api.openstack.compute.servers.Controller.create",
            before=lambda arg:
                "%s received" % f_ins_name(arg),
            after=lambda arg:
                "%s api returned" % f_ins_name(arg),
            excep=lambda arg:
                "%s failed: %s" % (f_ins_name(arg), arg['exc_val']),
            Release.KILO)

        self.register_point(
            "nova.api.openstack.compute.plugins.v3.servers.ServersController.create",
            before=lambda arg:
                "%s received" % f_ins_name(arg),
            after=lambda arg:
                "%s api returned" % f_ins_name(arg),
            excep=lambda arg:
                "%s failed: %s" % (f_ins_name(arg), arg['exc_val']),
            Release.KILO)

        self.register_point(
            "nova.api.openstack.compute.servers.ServersController.create",
            before=lambda arg:
                "%s received" % f_ins_name(arg),
            after=lambda arg:
                "%s api returned" % f_ins_name(arg),
            excep=lambda arg:
                "%s failed: %s" % (f_ins_name(arg), arg['exc_val']),
            [Release.MITAKA, Release.PROTOTYPE, Release.LATEST])

        self.register_point(
            "nova.conductor.rpcapi.ComputeTaskAPI.build_instances",
            before=lambda arg:
                "%s sent/retried" % arg['instances'][0].display_name)

        self.register_point(
            "nova.conductor.manager.ComputeTaskManager.build_instances",
            before=lambda arg:
                "%s,%s received" % (arg['instances'][0].display_name,
                                    arg['instances'][0].uuid))

        self.register_point(
            "nova.scheduler.utils.populate_retry",
            before=lambda arg:
                "%s attempts %s")

    def _inject_logs(self):
        # nova api part
        _from_create = lambda arg: arg['body']['server']['name']
        if self.release == Release.KILO:
            # there're two api entry points for "create"
            create_point = \
                "nova.api.openstack.compute.servers.Controller.create"
            self.patch_aop(
                    create_point,
                    before=lambda arg:
                        self.warn("%s received" % _from_create(arg)),
                    after=lambda arg:
                        self.warn("%s failed: %r"
                                  % (_from_create(arg), arg['exc_val']))
                        if "exc_val" in arg else
                        self.warn("%s api returned" % _from_create(arg)))
            create_point = \
                "nova.api.openstack.compute.plugins.v3.servers.ServersController.create"
        else:
            # mitaka, proto
            create_point = \
                "nova.api.openstack.compute.servers.ServersController.create"
        self.patch_aop(
                create_point,
                before=lambda arg:
                    self.warn("%s received" % _from_create(arg)),
                after=lambda arg:
                    self.warn("%s failed: %r"
                              % (_from_create(arg), arg['exc_val']))
                    if "exc_val" in arg else
                    self.warn("%s api returned" % _from_create(arg)))
        self.patch_aop(
                "nova.conductor.rpcapi.ComputeTaskAPI.build_instances",
                before=lambda arg:
                    self.warn("%s sent/retried"
                              % arg['instances'][0].display_name))

        # nova conductor part
        self.patch_aop(
                "nova.conductor.manager.ComputeTaskManager.build_instances",
                before=lambda arg:
                    self.warn("%s,%s received"
                              % (arg['instances'][0].display_name,
                                 arg['instances'][0].uuid)))
        self.patch_aop(
                "nova.scheduler.utils.populate_retry",
                after=lambda arg:
                    self.warn("%s failed: attempt 1" % arg["instance_uuid"])
                    if "exc_val" in arg and 'retry' not in arg["filter_properties"] else
                    self.warn("%s failed: attempt %s"
                              % (arg["instance_uuid"], arg["filter_properties"]['retry']['num_attempts']))
                    if "exc_val" in arg else
                    self.warn("%s attempts 1" % arg["instance_uuid"])
                    if 'retry' not in arg["filter_properties"] else
                    self.warn("%s attempts %s"
                              % (arg["instance_uuid"], arg["filter_properties"]['retry']['num_attempts'])))

        if self.release == Release.KILO:
            _from_spec = \
                lambda arg: arg["spec_obj"]["instance_properties"]["uuid"]
        else:
            # mitaka, proto
            _from_spec = lambda arg: arg["spec_obj"].instance_uuid

        def _get_host(kwargs):
            if self.release == Release.PROTOTYPE:
                return kwargs['ret_val'][0][0]['host']
            else:
                # mitaka, kilo
                return kwargs['ret_val'][0]['host']

        # TODO: except changed novalidhost
        self.patch_aop(
                "nova.scheduler.rpcapi.SchedulerAPI.select_destinations",
                before=lambda arg:
                    self.warn("%s sent scheduler" % (_from_spec(arg))),
                after=lambda arg:
                    self.warn("%s failed: %r"
                              % (_from_spec(arg), arg['exc_val']))
                    if "exc_val" in arg else
                    self.warn("%s decided %s"
                              % (_from_spec(arg),
                                 _get_host(arg))))
        self.patch_aop(
                "nova.compute.rpcapi.ComputeAPI.build_and_run_instance",
                before=lambda arg:
                    self.warn("%s sent %s"
                              % (arg['instance'].display_name,
                                 arg['node'])))

        # nova scheduler part
        if self.release == Release.KILO:
            _from_spec_kw = \
                lambda arg: \
                arg['request_spec']['instance_properties']["uuid"]
        else:
            # mitaka, proto
            _from_spec_kw = lambda arg: arg['spec_obj'].instance_uuid
        self.patch_aop(
                "nova.scheduler.manager.SchedulerManager.select_destinations",
                before=lambda arg:
                    self.warn("%s received" % _from_spec_kw(arg)),
                after=lambda arg:
                    self.warn("%s failed: %r"
                              % (_from_spec_kw(arg), arg['exc_val']))
                    if "exc_val" in arg else
                    self.warn("%s selected %s"
                              % (_from_spec_kw(arg), _get_host(arg))))
        self.patch_aop(
                "nova.scheduler.filter_scheduler.FilterScheduler._schedule",
                before=lambda arg:
                    self.warn("%s start scheduling" % _from_spec(arg)),
                after=lambda arg:
                    self.warn("%s finish scheduling" % _from_spec(arg)))
        self.patch_aop(
                "nova.scheduler.filter_scheduler."
                "FilterScheduler._get_all_host_states",
                before=lambda arg: self.warn("-- start_db"),
                after=lambda arg: self.warn("-- finish_db"))
        self.patch_aop(
                "nova.scheduler.caching_scheduler."
                "CachingScheduler._get_all_host_states",
                before=lambda arg: self.warn("-- start_db"),
                after=lambda arg: self.warn("-- finish_db"))

        # nova compute part
        c_m_patch = None
        if self.release == Release.KILO:
            c_m_patch = lambda arg: \
                self.warn("%s received" % arg["instance"].display_name)
        else:
            # mitaka, proto
            c_m_patch = lambda arg: \
                self.warn("%s received" % arg['instance'].display_name)

        self.patch_aop(
                "nova.compute.manager.ComputeManager.build_and_run_instance",
                before=c_m_patch)
        self.patch_aop(
                "nova.compute.manager.ComputeManager."
                "_do_build_and_run_instance",
                after=lambda arg:
                    self.warn("%s finished: %s"
                              % (arg["instance"].display_name, arg['ret_val'])))
        # TODO: except changed retry
        self.patch_aop(
                "nova.compute.manager.ComputeManager._build_and_run_instance",
                after=lambda arg:
                    self.warn("%s fail: %r" % (arg["instance"].display_name, arg['exc_val']))
                    if "exc_val" in arg else
                    self.warn("%s success" % arg["instance"].display_name))
        """
        self.patch_aop(
                "nova.compute.claims.Claim._claim_test",
                after=lambda *args, **kwargs:
                    self.warn("%s success"
                        % args[0].instance.display_name)
                    if kwargs['exc_val'] is None else
                    self.warn("%s fail: retry"
                        % args[0].instance.display_name))
        """


# TODO: implement this in the metaclass
register_driver("driver_scheduler", BenchDriverScheduler)
