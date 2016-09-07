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

from nova import exception

from openstack_bench.bench_drivers import register_driver
from openstack_bench.config import CONF_BENCH
from openstack_bench.releases import Release

import bases


def debug(*args, **kwargs):
    import pdb
    pdb.set_trace()


class BenchDriverScheduler(bases.BenchDriverBase):
    def __init__(self, meta, host_manager=None):
        # TODO: handle releases in nova_patcher
        self.release = Release[CONF_BENCH.nova_patcher.release]
        super(BenchDriverScheduler, self).__init__(meta)

    def _inject_logs(self):
        # nova api part
        _from_create = lambda kwargs: kwargs['body']['server']['name']
        if self.release == Release.KILO:
            # there're two api entry points for "create"
            create_point = \
                "nova.api.openstack.compute.servers.Controller.create"
            self.patch_aop(
                    create_point,
                    before=lambda *args, **kwargs:
                        self.warn("%s received" % _from_create(kwargs)),
                    after=lambda *args, **kwargs:
                        self.warn("%s failed: %s"
                                  % (_from_create(kwargs), kwargs['exc_val']))
                        if kwargs['exc_val'] else
                        self.warn("%s api returned" % _from_create(kwargs)))
            create_point = \
                "nova.api.openstack.compute.plugins.v3.servers.ServersController.create"
        else:
            # mitaka, proto
            create_point = \
                "nova.api.openstack.compute.servers.ServersController.create"
        self.patch_aop(
                create_point,
                before=lambda *args, **kwargs:
                    self.warn("%s received" % _from_create(kwargs)),
                after=lambda *args, **kwargs:
                    self.warn("%s failed: %s"
                              % (_from_create(kwargs), kwargs['exc_val']))
                    if kwargs['exc_val'] else
                    self.warn("%s api returned" % _from_create(kwargs)))
        self.patch_aop(
                "nova.conductor.rpcapi.ComputeTaskAPI.build_instances",
                before=lambda *args, **kwargs:
                    self.warn("%s sent/retried"
                              % kwargs['instances'][0].display_name))

        # nova conductor part
        self.patch_aop(
                "nova.conductor.manager.ComputeTaskManager.build_instances",
                before=lambda *args, **kwargs:
                    self.warn("%s,%s received"
                              % (kwargs['instances'][0].display_name,
                                 kwargs['instances'][0].uuid)))
        self.patch_aop(
                "nova.scheduler.utils.populate_retry",
                after=lambda *args, **kwargs:
                    self.warn("%s failed: attempt 1" % args[1])
                    if kwargs['exc_val'] and 'retry' not in args[0] else
                    self.warn("%s failed: attempt %s"
                              % (args[1], args[0]['retry']['num_attempts']))
                    if kwargs['exc_val'] else
                    self.warn("%s attempts 1" % args[1])
                    if 'retry' not in args[0] else
                    self.warn("%s attempts %s"
                              % (args[1], args[0]['retry']['num_attempts'])))

        if self.release == Release.KILO:
            _from_spec = \
                lambda args: args[2]["instance_properties"]["uuid"]
        else:
            # mitaka, proto
            _from_spec = lambda args: args[2].instance_uuid

        def _get_host(kwargs):
            if self.release == Release.PROTOTYPE:
                return kwargs['ret_val'][0][0]['host']
            else:
                # mitaka, kilo
                return kwargs['ret_val'][0]['host']

        self.patch_aop(
                "nova.scheduler.rpcapi.SchedulerAPI.select_destinations",
                before=lambda *args, **kwargs:
                    self.warn("%s sent scheduler" % (_from_spec(args))),
                after=lambda *args, **kwargs:
                    self.warn("%s failed: novalidhost" % (_from_spec(args)))
                    if isinstance(kwargs['exc_val'], exception.NoValidHost)
                    else
                    self.warn("%s failed: %s"
                              % (_from_spec(args), kwargs['exc_val']))
                    if kwargs['exc_val'] else
                    self.warn("%s decided %s"
                              % (_from_spec(args),
                                 _get_host(kwargs))))
        self.patch_aop(
                "nova.compute.rpcapi.ComputeAPI.build_and_run_instance",
                before=lambda *args, **kwargs:
                    self.warn("%s sent %s"
                              % (kwargs['instance'].display_name,
                                 kwargs['node'])))

        # nova scheduler part
        if self.release == Release.PROTOTYPE:
            _from_spec_kw = \
                lambda kwargs: \
                kwargs['request_spec']['instance_properties']["uuid"]
        else:
            # mitaka, proto
            _from_spec_kw = lambda kwargs: kwargs['spec_obj'].instance_uuid
        self.patch_aop(
                "nova.scheduler.manager.SchedulerManager.select_destinations",
                before=lambda *args, **kwargs:
                    self.warn("%s received" % _from_spec_kw(kwargs)),
                after=lambda *args, **kwargs:
                    self.warn("%s failed: novalidhost"
                              % _from_spec_kw(kwargs))
                    if kwargs['exc_val'] else
                    self.warn("%s selected %s"
                              % (_from_spec_kw(kwargs),
                                 _get_host(kwargs))))
        self.patch_aop(
                "nova.scheduler.filter_scheduler.FilterScheduler._schedule",
                before=lambda *args, **kwargs:
                    self.warn("%s start scheduling" % _from_spec(args)),
                after=lambda *args, **kwargs:
                    self.warn("%s finish scheduling" % _from_spec(args)))
        self.patch_aop(
                "nova.scheduler.filter_scheduler."
                "FilterScheduler._get_all_host_states",
                before=lambda *args, **kwargs: self.warn("-- start_db"),
                after=lambda *args, **kwargs: self.warn("-- finish_db"))
        self.patch_aop(
                "nova.scheduler.caching_scheduler."
                "CachingScheduler._get_all_host_states",
                before=lambda *args, **kwargs: self.warn("-- start_db"),
                after=lambda *args, **kwargs: self.warn("-- finish_db"))

        # nova compute part
        c_m_patch = None
        if self.release == Release.KILO:
            c_m_patch = lambda *args, **kwargs: \
                self.warn("%s received" % args[2].display_name)
        else:
            # mitaka, proto
            c_m_patch = lambda *args, **kwargs: \
                self.warn("%s received" % kwargs['instance'].display_name)

        self.patch_aop(
                "nova.compute.manager.ComputeManager.build_and_run_instance",
                before=c_m_patch)
        self.patch_aop(
                "nova.compute.manager.ComputeManager."
                "_do_build_and_run_instance",
                after=lambda *args, **kwargs:
                    self.warn("%s finished: %s"
                              % (args[2].display_name, kwargs['ret_val'])))
        self.patch_aop(
                "nova.compute.manager.ComputeManager._build_and_run_instance",
                after=lambda *args, **kwargs:
                    self.warn("%s fail: retry" % args[2].display_name)
                    if isinstance(kwargs['exc_val'], exception.RescheduledException) else
                    self.warn("%s fail: %s" % (args[2].display_name, kwargs['exc_val']))
                    if kwargs['exc_val'] is not None else
                    self.warn("%s success" % args[2].display_name))
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
