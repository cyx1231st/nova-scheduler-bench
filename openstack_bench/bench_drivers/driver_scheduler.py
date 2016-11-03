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

        master.get_graph(12, 22).name = "conductor1"
        master.get_graph(11, 13).name = "conductor1"

        return master

    def build_statistics(self, s_engine, report):
        _i_apif = frozenset([(0, 20)])
        _i_api = frozenset([(0, 20), (0, 2)])
        _i_apis = frozenset([(0, 2)])
        _i_atc = frozenset([(1, 3)])
        _i_con1 = frozenset([(2, 5), (2, 21)])
        _i_cts = frozenset([(4, 6)])
        _i_sch = frozenset([(5, 11), (5, 12)])
        _i_stc = frozenset([(10, 13), (10, 22)])
        _i_con2 = frozenset([(11, 14)])
        _i_contc = frozenset([(13, 15)])
        _i_com = frozenset([(14, 2), (14, 23), (14, 25), (14, 26)])
        _i_con = frozenset(_i_con1 | _i_con2)

        _i_fil = frozenset([(6, 10)])
        _i_cac = frozenset([(7, 9)])
        _i_gap = frozenset([(8, 16), (8, 23), (8, 24)])
        _i_sus = frozenset([(0, 25)])
        _i_nvh = frozenset([(0, 22)])
        _i_ret = frozenset([(0, 16)])

        _i_all = [_i_api, _i_con, _i_sch, _i_com]
        _i_cut = [_i_api, _i_con1, _i_con2, _i_sch, _i_com, _i_atc, _i_cts,
                  _i_stc, _i_contc, _i_fil, _i_cac, _i_gap, _i_sus, _i_apif,
                  _i_nvh, _i_ret, _i_apis]
        cut_edge = s_engine.graph.get_edge(16, 2)

        all_, cut, cutted = s_engine.extract_intervals(_i_all, _i_cut, cut_edge)

        report.register("active schedulers",
                        s_engine.active_by_service.get("scheduler", 0))
        report.register("active computes",
                        s_engine.active_by_service.get("compute", 0))
        report.blank()
        report.register("total requests",
                        s_engine.total_requests)
        report.register("success requests",
                        s_engine.requests_by_state.get("SUCCESS", 0))
        report.register("nvh requests",
                        s_engine.requests_by_state.get("NO VALID HOST", 0))
        report.register("rtf requests",
                        s_engine.requests_by_state.get("RETRY FAIL", 0))
        report.register("api fail requests",
                        s_engine.requests_by_state.get("API FAIL", 0))
        report.register("compute fail requests",
                        s_engine.requests_by_state.get("COMPUTE FAIL", 0))
        report.register("error requests",
                        s_engine.requests_by_state.get("PARSE ERROR", 0))
        report.blank()
        report.register("total valid queries",
                        s_engine.count(3))
        report.register("direct successful queries",
                        len(cut[_i_sus]))
        report.register("direct nvh queries",
                        len(cut[_i_nvh]))
        report.register("direct retried queries",
                        len(cut[_i_ret]))
        report.register("retry successful queries",
                        s_engine.count(25) - len(cut[_i_sus]))
        report.register("retry nvh queries",
                        s_engine.count(22) - len(cut[_i_nvh]))
        report.register("retry retried queries",
                        s_engine.count(16) - len(cut[_i_ret]))
        report.blank()
        report.register("wallclock total",
                        s_engine.intervals_requests.wall_time())
        report.register("wallclock api",
                        s_engine.intervals_by_services["api"].wall_time())
        report.register("wallclock conductor",
                        s_engine.intervals_by_services["conductor"].wall_time())
        report.register("wallclock scheduler",
                        s_engine.intervals_by_services["scheduler"].wall_time())
        report.register("wallclock compute",
                        s_engine.intervals_by_services["compute"].wall_time())
        report.blank()
        report.register("time query avg", cut[_i_sus].average())
        report.register("time inapi avg", cut[_i_api].average())
        report.register("time a-con avg", cut[_i_atc].average())
        report.register("time cond1 avg", cut[_i_con1].average())
        report.register("time c-sch avg", cut[_i_cts].average())
        report.register("time sched avg", cut[_i_sch].average())
        report.register("time s-con avg", cut[_i_stc].average())
        report.register("time cond2 avg", cut[_i_con2].average())
        report.register("time c-com avg", cut[_i_contc].average())
        report.register("time compu avg", cut[_i_com].average())
        report.blank()
        report.register("time filter avg", cut[_i_fil].average())
        report.register("time refresh avg", cut[_i_cac].average())
        report.register("time gap avg", cut[_i_gap].average())
        report.blank()
        sum_query_avg = cut[_i_api].average()\
                        + cut[_i_atc].average()\
                        + cut[_i_con1].average()\
                        + cut[_i_cts].average()\
                        + cut[_i_sch].average()\
                        + cut[_i_stc].average()\
                        + cut[_i_con2].average()\
                        + cut[_i_contc].average()\
                        + cut[_i_com].average()
        report.register("percent api part",
                        cut[_i_api].average() / sum_query_avg * 100)
        report.register("percent cond part",
                        (cut[_i_con1].average() + cut[_i_con2].average())
                        / sum_query_avg * 100)
        report.register("percent sch part",
                        cut[_i_sch].average() / sum_query_avg * 100)
        report.register("percent comp part",
                        cut[_i_com].average() / sum_query_avg * 100)
        report.register("percent msg part",
                        (cut[_i_atc].average() + cut[_i_cts].average()
                         + cut[_i_stc].average() + cut[_i_contc].average())
                        / sum_query_avg * 100)
        report.register("percent filter part",
                        cut[_i_fil].average() / sum_query_avg * 100)
        report.register("percent refresh part",
                        cut[_i_cac].average() / sum_query_avg * 100)
        report.register("percent gap part",
                        cut[_i_gap].average() / sum_query_avg * 100)
        report.blank()
        report.register("request per sec",
                        (s_engine.total_requests -
                         s_engine.requests_by_state.get("API FAIL", 0))
                        / s_engine.intervals_requests.wall_time())
        report.register("query per sec",
                        s_engine.count(3)
                        / s_engine.intervals_requests.wall_time())
        report.register("success per sec",
                        s_engine.requests_by_state.get("SUCCESS", 0)
                        / s_engine.intervals_requests.wall_time())
        report.blank()
        report.register("percent query retry",
                        s_engine.count(16)
                        / float(s_engine.count(1) - s_engine.count(20)) * 100)
        report.register("percent api fail",
                        s_engine.count(20) / float(s_engine.count(1)) * 100)


# TODO: implement this in the metaclass
register_driver("driver_scheduler", BenchDriverScheduler)
