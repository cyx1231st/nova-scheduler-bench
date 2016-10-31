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
from openstack_bench.log_parser.state_graph import MasterGraph
from openstack_bench.releases import Release

import bases


def debug(*args, **kwargs):
    import pdb
    pdb.set_trace()


class BenchDriverScheduler(bases.BenchDriverBase):
    SERVICES = {"api", "conductor", "scheduler", "compute"}

    def __init__(self):
        super(BenchDriverScheduler, self).__init__()

    def register_points(self):
        f_ins_name = lambda arg: arg['body']['server']['name']

        self.register_point(
            "nova.api.openstack.compute.servers.Controller.create",
            before=lambda arg:
                "%s received" % f_ins_name(arg),
            after=lambda arg:
                "%s api returned" % f_ins_name(arg),
            excep=lambda arg:
                "%s failed: %r" % (f_ins_name(arg), arg['exc_val']),
            release=Release.KILO)

        self.register_point(
            "nova.api.openstack.compute.plugins.v3."
            "servers.ServersController.create",
            before=lambda arg:
                "%s received" % f_ins_name(arg),
            after=lambda arg:
                "%s api returned" % f_ins_name(arg),
            excep=lambda arg:
                "%s failed: %r" % (f_ins_name(arg), arg['exc_val']),
            release=Release.KILO)

        self.register_point(
            "nova.api.openstack.compute.servers.ServersController.create",
            before=lambda arg:
                "%s received" % f_ins_name(arg),
            after=lambda arg:
                "%s api returned" % f_ins_name(arg),
            excep=lambda arg:
                "%s failed: %r" % (f_ins_name(arg), arg['exc_val']),
            release=[Release.MITAKA, Release.PROTOTYPE, Release.LATEST])

        self.register_point(
            "nova.conductor.rpcapi.ComputeTaskAPI.build_instances",
            before=lambda arg:
                "%s sent/retried" % arg['instances'][0].display_name)

        self.register_point(
            "nova.conductor.manager.ComputeTaskManager.build_instances",
            before=lambda arg:
                "%s,%s received" % (arg['instances'][0].display_name,
                                    arg['instances'][0].uuid))

        def get_attempts(arg):
            if "retry" not in arg["filter_properties"]:
                return 1
            else:
                return arg["filter_properties"]['retry']['num_attempts']

        self.register_point(
            "nova.scheduler.utils.populate_retry",
            after=lambda arg:
                "%s attempts %s" % (arg['instance_uuid'],
                                    get_attempts(arg)),
            excep=lambda arg:
                "%s failed: attempt %s" % (arg['instance_uuid'],
                                           get_attempts(arg)))

        def from_spec(arg):
            if self.release == Release.KILO:
                return arg["spec_obj"]["instance_properties"]["uuid"]
            else:
                return arg["spec_obj"].instance_uuid

        def get_host(arg):
            if self.release == Release.PROTOTYPE:
                return arg['ret_val'][0][0]['host']
            else:
                # mitaka, kilo
                return arg['ret_val'][0]['host']

        self.register_point(
            "nova.scheduler.rpcapi.SchedulerAPI.select_destinations",
            before=lambda arg:
                "%s sent scheduler" % from_spec(arg),
            after=lambda arg:
                "%s decided %s" % (from_spec(arg), get_host(arg)),
            excep=lambda arg:
                "%s failed: %r" % (from_spec(arg), arg['exc_val']))

        self.register_point(
            "nova.compute.rpcapi.ComputeAPI.build_and_run_instance",
            before=lambda arg:
                "%s sent %s" % (arg['instance'].display_name, arg['node']))

        def from_spec1(arg):
            if self.release == Release.KILO:
                return arg['request_spec']['instance_properties']["uuid"]
            else:
                return arg['spec_obj'].instance_uuid

        self.register_point(
            "nova.scheduler.manager.SchedulerManager.select_destinations",
            before=lambda arg:
                "%s received" % from_spec1(arg),
            after=lambda arg:
                "%s selected %s" % (from_spec1(arg), get_host(arg)),
            excep=lambda arg:
                "%s failed: %r" % (from_spec1(arg), arg['exc_val']))

        self.register_point(
            "nova.scheduler.filter_scheduler.FilterScheduler._schedule",
            before=lambda arg:
                "%s start scheduling" % from_spec(arg),
            after=lambda arg:
                "%s finish scheduling" % from_spec(arg))

        self.register_point(
            "nova.scheduler.filter_scheduler."
            "FilterScheduler._get_all_host_states",
            before=lambda arg: "-- start_db",
            after=lambda arg: "-- finish_db")

        self.register_point(
            "nova.scheduler.caching_scheduler."
            "CachingScheduler._get_all_host_states",
            before=lambda arg: "-- start_db",
            after=lambda arg: "-- finish_db")

        self.register_point(
            "nova.compute.manager.ComputeManager.build_and_run_instance",
            before=lambda arg:
                "%s received" % arg['instance'].display_name)

        self.register_point(
            "nova.compute.manager.ComputeManager._do_build_and_run_instance",
            after=lambda arg:
                "%s finished: %s" % (arg['instance'].display_name,
                                     arg['ret_val']))

        self.register_point(
            "nova.compute.manager.ComputeManager._build_and_run_instance",
            after=lambda arg:
                "%s success" % arg["instance"].display_name,
            excep=lambda arg:
                "%s fail: %r" % (arg["instance"].display_name, arg['exc_val']))

    def build_graph(self):
        master = MasterGraph("master")
        build = master.build

        build(0, 1, "api", "received")
        build(1, 20, "api", "failed:")
        build(1, 2, "api", "sent/retried")
        build(2, 3, "conductor", "received")
        build(3, 21, "conductor", "failed: attempt")
        build(3, 4, "conductor", "attempts")
        build(4, 5, "conductor", "sent scheduler")
        build(5, 6, "scheduler", "received")
        build(6, 7, "scheduler", "start scheduling")
        build(7, 8, "scheduler", "start_db")
        build(8, 9, "scheduler", "finish_db")
        build(9, 10, "scheduler", "finish scheduling")
        build(10, 11, "scheduler", "selected")
        build(10, 12, "scheduler", "failed:")
        build(12, 22, "conductor", "failed: NoValidHost")
        build(11, 13, "conductor", "decided")
        build(13, 14, "conductor", "sent")
        build(14, 15, "compute", "received")
        build(15, 24, "compute", "success")
        build(24, 25, "compute", "finished: active")
        # TODO: change to "fail: Rescheduled"
        # build(15, 16, "compute", "fail: Rescheduled")
        build(15, 16, "compute", "fail: retry")
        build(15, 23, "compute", "fail:")
        build(15, 26, "compute", "finished: None")
        build(16, 2, "compute", "sent/retried")

        master.get_graph(0, 1).add_ignore_edge("api", "api returned")
        master.get_graph(14, 15).add_ignore_edge("compute", "finished:")

        edge = master.get_edge(13, 14)
        edge.f_assume_host = lambda action: action.split(" ")[1]

        master.set_state(20, "API FAIL")
        master.set_state(21, "RETRY FAIL")
        master.set_state(22, "NO VALID HOST")
        master.set_state(23, "COMPUTE FAIL")
        master.set_state(25, "SUCCESS")
        master.set_state(26, "COMPUTE FAIL")

        print master
        for sub in master.graphs:
            print sub

        return master


# TODO: implement this in the metaclass
register_driver("driver_scheduler", BenchDriverScheduler)
