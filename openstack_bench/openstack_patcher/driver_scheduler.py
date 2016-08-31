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
from nova.network import model as network_model
from nova.virt import fake

import bench


def get_view(v_type):
    if v_type == "A":
        return ComputeResourceView(None,
                                   vcpus=8,
                                   memory_mb=64*1024,  # 64G
                                   local_gb=25*1024,   # 25TB
                                   r_ratio=1.5,
                                   c_ratio=16,
                                   d_ratio=1.5)
    elif v_type == "B":  # cannot place flavor 154
        return ComputeResourceView(None,
                                   vcpus=3,
                                   memory_mb=4*1024,
                                   local_gb=4*1024)
    elif v_type == "C":  # can only fit 1 host for flavor 154
        return ComputeResourceView(None,
                                   vcpus=6,
                                   memory_mb=12*1024,
                                   local_gb=4*1024)
    else:
        raise RuntimeError("Unsupported view option: %s" % v_type)


class ComputeResourceView(object):
    def __init__(self,
                 hostname,
                 vcpus=4,
                 memory_mb=2048,
                 local_gb=1024,
                 r_ratio=1.0,
                 c_ratio=1.0,
                 d_ratio=1.0):
        self.hostname = hostname
        self.vcpus = vcpus
        self.memory_mb = memory_mb
        self.local_gb = local_gb
        self.r_ratio = r_ratio
        self.c_ratio = c_ratio
        self.d_ratio = d_ratio


class SchedulerFakeDriver(fake.FakeDriver):
    vcpus = 4
    memory_mb = 8192
    local_gb = 1024

    @classmethod
    def prepare_resource(cls, resource):
        if resource:
            cls.vcpus = resource.vcpus
            cls.memory_mb = resource.memory_mb
            cls.local_gb = resource.local_gb

    def get_available_resource(self, nodename):
        host_state = super(SchedulerFakeDriver, self)\
                .get_available_resource(nodename)
        host_state['disk_available_least'] = \
                host_state['local_gb'] - host_state['local_gb_used']
        return host_state


fake_async_networkinfo = \
    lambda *args, **kwargs: network_model.NetworkInfoAsyncWrapper(
        lambda *args, **kwargs: network_model.NetworkInfo())


fake_deallocate_networkinfo = lambda *args, **kwargs: None


fake_check_requested_networks = lambda *args, **kwargs: 1


def debug(*args, **kwargs):
    import pdb
    pdb.set_trace()


class BenchDriverScheduler(bench.BenchDriverBase):
    def __init__(self, meta, host_manager=None):
        self.meta = meta
        if self.meta.scheduler_type == "filter":
            self.host_manager = "host_manager"
            self.scheduler_driver = "filter_scheduler"
        elif self.meta.scheduler_type == "caching":
            self.host_manager = "host_manager"
            self.scheduler_driver = "caching_scheduler"
        elif self.meta.scheduler_type == "shared":
            self.host_manager = "shared_host_manager"
            self.scheduler_driver = "filter_scheduler"
        else:
            raise RuntimeError("Unsupported scheduler type: %s"
                               % self.meta.scheduler_type)
        self.resource = get_view(self.meta.v_type)
        self.resource.hostname = self.meta.host
        if meta.release not in ["mitaka+", "mitaka", "kilo", "proto"]:
            raise RuntimeError("Unsupported release: %s" % meta.release)
        self.release = meta.release
        super(BenchDriverScheduler, self).__init__(meta)

    def _stubout_nova(self):
        resource_view = self.resource
        SchedulerFakeDriver.prepare_resource(resource_view)
        self.patch('nova.virt.fake.SchedulerFakeDriver',
                   SchedulerFakeDriver,
                   add=True)
        self.patch('nova.compute.manager.ComputeManager._allocate_network',
                   fake_async_networkinfo)
        self.patch('nova.compute.manager.ComputeManager._deallocate_network',
                   fake_deallocate_networkinfo)
        self.patch('nova.compute.api.API._check_requested_networks',
                   fake_check_requested_networks)

    def _stubout_conf(self):
        if self.release == "mitaka+":
            self.conf("compute_driver",
                      'fake.SchedulerFakeDriver')
        else:
            self.conf("compute_driver",
                      'nova.virt.fake.SchedulerFakeDriver')

        if self.release != "kilo":
            self.conf("ram_allocation_ratio", self.resource.r_ratio)
            self.conf("cpu_allocation_ratio", self.resource.c_ratio)
            self.conf("disk_allocation_ratio", self.resource.d_ratio)
            self.conf("scheduler_driver", self.scheduler_driver)
            self.conf("scheduler_host_manager", self.host_manager)
        self.conf("reserved_host_disk_mb", 0)
        self.conf("reserved_host_memory_mb", 0)
        self.conf("scheduler_max_attempts", 5)
        self.conf("scheduler_default_filters",
                  ["RetryFilter",
                   "AvailabilityZoneFilter",
                   "RamFilter",
                   "DiskFilter",
                   # "CoreFilter",
                   "ComputeFilter",
                   "ComputeCapabilitiesFilter",
                   "ImagePropertiesFilter",
                   "ServerGroupAntiAffinityFilter",
                   "ServerGroupAffinityFilter"])

    def _inject_logs(self):
        # nova api part
        _from_create = lambda kwargs: kwargs['body']['server']['name']
        if self.release == "kilo":
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

        if self.release == "kilo":
            _from_spec = \
                lambda args: args[2]["instance_properties"]["uuid"]
        else:
            # mitaka, proto
            _from_spec = lambda args: args[2].instance_uuid

        def _get_host(kwargs):
            if self.host_manager == "shared_host_manager" \
                    or self.release == "proto":
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
        if self.release == "kilo":
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
        if self.release == "kilo":
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


def get_driver(meta):
    return BenchDriverScheduler(meta)
