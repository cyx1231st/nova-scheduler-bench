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

from __future__ import print_function

import argparse
import collections
from itertools import chain
import os
from os import path

SECONDS_OFFSET=False
C_SCHEDULER=False


class LogLine(object):
    def __init__(self, line, service, host, filename, offset):
        self.filename = filename
        self.line = line
        pieces = line.split()
        time_pieces = pieces[1].split(":")
        if SECONDS_OFFSET:
            seconds = float(time_pieces[2])
            seconds_int = int(seconds)
            seconds_fraction = (seconds - seconds_int) * 10
            if seconds_fraction >= 1:
                raise RuntimeError("seconds fraction must < 1!")
            seconds = seconds_int + seconds_fraction
            self.seconds = int(time_pieces[0]) * 3600 + \
                int(time_pieces[1]) * 60 + \
                float(seconds)
        else:
            self.seconds = int(time_pieces[0]) * 3600 + \
                int(time_pieces[1]) * 60 + \
                float(time_pieces[2])
        if service == "compute" and offset:
            self.seconds += offset
        self.time = pieces[1]
        self.request_id = pieces[4][5:]

        instance_info = pieces[8]
        self.instance_id = "?"
        self.instance_name = "?"
        if instance_info == "--":
            pass
        elif "," in instance_info:
            instance_info = instance_info.split(",")
            self.instance_name = instance_info[0]
            self.instance_id = instance_info[1]
        elif len(instance_info) is 36:
            self.instance_id = instance_info
        else:
            self.instance_name = instance_info

        self.service = service
        self.host = host
        self.action = " ".join(pieces[9:])

    def __repr__(self):
        return self.time + " " + \
            self.service + " " + \
            self.host + " " + \
            self.request_id + " " + \
            self.instance_id + " " + \
            self.instance_name + " " + \
            self.action

    def get_relation(self, relation):
        if self.instance_id == "?" or self.instance_name == "?":
            return True

        rel = Relation(self.request_id, self.instance_id, self.instance_name,
                       self.line, self.filename)
        rel_record = relation.get(rel.instance_id, None)
        if not rel_record:
            relation[rel.instance_id] = rel
            return True
        else:
            if rel.instance_id != rel_record.instance_id or \
                    rel.instance_name != rel_record.instance_name:
                print("Mismatch: %s, %s" %
                      (rel_record.get_line(), rel.get_line()))
                return False
            return True

    def assert_c(self, service, key_word):
        if self.service != service:
            return False
        if key_word not in self.action:
            return False
        return True

    def apply(self, relation_id, relation_name, log_lines, i):
        if self.instance_id == "?" and self.instance_name == "?":
            if "start_db" in self.action:
                if "start scheduling" in log_lines[i-1].action:
                    self.instance_id = log_lines[i-1].instance_id
                    if self.instance_id == "?":
                        return False
                else:
                    print("Unable to resolve instance relations1: %s!"
                          % self.get_line())
                    return False
            elif "finish_db" in self.action:
                if "finish scheduling" in log_lines[i+1].action:
                    self.instance_id = log_lines[i+1].instance_id
                    if self.instance_id == "?":
                        return False
                else:
                    print("Unable to resolve instance relations2: %s!"
                          % self.get_line())
                    return False
            else:
                print("Unable to resolve instance relations: %s!"
                      % self.get_line())
                return False

        if self.instance_id == "?":
            rel = relation_name[self.instance_name]
            self.instance_id = rel.instance_id
            return True
        elif self.instance_name == "?":
            rel = relation_id[self.instance_id]
            self.instance_name = rel.instance_name
            return True
        else:
            return True

    def get_line(self):
        return self.filename + ": " + self.line


class Relation(object):
    def __init__(self, r_id, i_id, i_name, line, filename):
        self.line = line
        self.filename = filename
        self.request_id = r_id
        self.instance_id = i_id
        self.instance_name = i_name

    def get_line(self):
        return self.filename + ": " + self.line


class StateMachine(object):
    NOTSTARTED = "NotStarted"
    INCOMPLETE = "Incomplete"
    FAILED = "Failed"
    ERROR = "Error"
    SUCCESS = "Success"

    def __init__(self, uuid, logs):
        self.uuid = uuid
        self.logs = logs
        self.state = 0
        self.retries = 1

        self.error_logs = []
        self.extra_logs = []
        self.cqs = []
        self.cqs_state = None   #success/retried/nvh/failed
        self.rqs_final = []
        self.rqs_state = None   #success/nvh/failed/retry
        self.rqs_retried_list = []

        self.status = self.NOTSTARTED

    def report(self):
        print("")
        print(">>> State Machine #%s" % self.uuid)
        print("  state: %d" % self.state)
        print("  retries: %d" % self.retries)
        print("  cps length: %d" % len(self.cqs))
        print("  cps state: %s" % self.cqs_state)
        count = 0
        if self.rqs_final:
            print("  last rqs state: %s" % self.rqs_state)
            count = 1
        print("  rqs count: %d" % (count+len(self.rqs_retried_list)))

        print(" <LOG>")
        for lg in self.logs:
            print(lg)
        if self.error_logs:
            print(" <ERRORS>")
            for lg in self.error_logs:
                print(lg)
        if self.extra_logs:
            print(" <EXTRA LOGS>")
            for lg in self.extra_logs:
                print(lg)
        print("Evaluation: %s" % self.status)

    def _parse_line(self, lg, queue, i, end):

        def _assert(service, retried, key_word):
            if retried is not None and \
                    ((self.retries == 1 and retried is True) or
                     (self.retries > 1 and retried is False) or
                     self.retries < 1):
                return False
            return lg.assert_c(service, key_word)

        def _fail():
            self.status = self.ERROR
            return False

        def _stop(next_):
            self.state = next_
            queue.append(lg)
            return False

        def _next(next_):
            self.state = next_
            queue.append(lg)
            return True

        def _extra():
            self.extra_logs.append(lg)
            return True

        if _assert("api", False, "api returned"):
            return _extra()
        elif _assert("compute", None, "finished:"):
            if self.state == 15 and i == end:
                return _stop(25)
            else:
                return _extra()

        state = self.state
        if state == 0:
            if _assert("api", False, "received"):
                return _next(1)
        elif state == 1:
            if _assert("api", False, "failed:"):
                return _stop(20)
            elif _assert("api", False, "sent/retried"):
                return _next(2)
        elif state == 2:
            if _assert("conductor", None, "received"):
                if self.retries == 1:
                    pass
                else:
                    if self.retries == 2:
                        self.cqs.extend(queue[:])
                    else:
                        self.rqs_retried_list.append(queue[:])
                    del queue[:]
                return _next(3)
        elif state == 3:
            if _assert("conductor", None, "failed: attempt"):
                return _stop(21)
            elif _assert("conductor", None, "attempts"):
                if int(lg.action.split(" ")[1]) == self.retries:
                    return _next(4)
        elif state == 4:
            if _assert("conductor", None, "sent scheduler"):
                return _next(5)
        elif state == 5:
            if _assert("scheduler", None, "received"):
                return _next(6)
        elif state == 6:
            if _assert("scheduler", None, "start scheduling"):
                return _next(7)
        elif state == 7:
            if _assert("scheduler", None, "start_db"):
                return _next(8)
            elif _assert("scheduler", None, "finish scheduling"):
                return _next(10)
        elif state == 8:
            if _assert("scheduler", None, "finish_db"):
                return _next(9)
        elif state == 9:
            if _assert("scheduler", None, "finish scheduling"):
                return _next(10)
        elif state == 10:
            if _assert("scheduler", None, "failed: novalidhost"):
                return _next(12)
            elif _assert("scheduler", None, "selected"):
                return _next(11)
        elif state == 12:
            if _assert("conductor", None, "failed: novalidhost"):
                return _stop(22)
            # NOTE(Yingxin)
            elif _assert("conductor", None,
                         "failed: Remote error: TimeoutError"):
                return _stop(22)
        elif state == 11:
            if _assert("conductor", None, "decided"):
                return _next(13)
        elif state == 13:
            if _assert("conductor", None, "sent"):
                return _next(14)
        elif state == 14:
            if _assert("compute", None, "received"):
                return _next(15)
        elif state == 15:
            if _assert("compute", None, "success"):
                return _stop(24)
            elif _assert("compute", None, "fail: retry"):
                return _next(16)
            elif _assert("compute", None, "fail:"):
                return _stop(23)
        elif state == 16:
            self.retries += 1
            if _assert("compute", None, "sent/retried"):
                return _next(2)

        return _fail()

    def parse(self):
        self.state = 0
        self.retries = 1
        queue = []

        def _incomplete():
            self.status = self.INCOMPLETE
            self.error_logs.extend(queue)

        def _fail(i):
            self.status = self.FAILED
            self.error_logs.extend(queue)
            self.error_logs.extend(self.logs[i+1:])

        def _stop(i, logs):
            self.status = self.SUCCESS
            logs.extend(queue)
            self.error_logs.extend(self.logs[i+1:])

            errors = []
            for lg in self.error_logs:
                if lg.assert_c("compute", "finished: active") or \
                   lg.assert_c("compute", "finished: rescheduled"):
                    self.extra_logs.append(lg)
                else:
                    errors.append(lg)
            self.error_logs = errors
                
        def _error(i):
            self.status = self.ERROR
            self.error_logs.extend(queue)
            self.error_logs.extend(self.logs[i+1:])

        for i in range(0, len(self.logs)):
            if not self._parse_line(self.logs[i], queue, i, len(self.logs)-1):
                if self.status == self.ERROR:
                    return _error(i)
                elif self.state == 20:
                    return _fail(i)
                elif self.state == 21:
                    if self.retries == 1:
                        return _fail(i)
                    else:
                        self.rqs_state = "max retry"
                        return _stop(i, self.rqs_final)
                elif self.state == 22:
                    if self.retries == 1:
                        self.cqs_state = "no valid host"
                        return _stop(i, self.cqs)
                    else:
                        self.rqs_state = "no valid host"
                        return _stop(i, self.rqs_final)
                elif self.state == 23:
                    if self.retries == 1:
                        self.cqs_state = "compute failed"
                        return _stop(i, self.cqs)
                    else:
                        self.rqs_state = "compute failed"
                        return _stop(i, self.rqs_final)
                elif self.state == 24:
                    if self.retries == 1:
                        self.cqs_state = "success"
                        return _stop(i, self.cqs)
                    else:
                        self.rqs_state = "success"
                        return _stop(i, self.rqs_final)
                elif self.state == 25:
                    if self.retries == 1:
                        self.cqs_state = "compute finished failed"
                        return _stop(i, self.cqs)
                    else:
                        self.rqs_state = "compute finished failed"
                        return _stop(i, self.rqs_final)
                else:
                    return _error(i)

        return _incomplete()


class StateMachinesParser(object):
    def __init__(self, state_machines):
        self.total_requests = state_machines
        self.success_requests = []
        self.nvh_requests = []
        self.retry_failed_requests = []
        self.api_failed_requests = []
        self.incomplete_requests = []
        self.error_requests = []

        for sm in state_machines.values():
            if sm.status == StateMachine.ERROR:
                self.error_requests.append(sm)
            elif sm.status == StateMachine.FAILED:
                self.api_failed_requests.append(sm)
            elif sm.status == StateMachine.INCOMPLETE:
                self.incomplete_requests.append(sm)
            elif sm.status == StateMachine.SUCCESS:
                state = sm.cqs_state
                if state is None:
                    state = sm.rqs_state
                if state == "success":
                    self.success_requests.append(sm)
                elif state == "no valid host":
                    self.nvh_requests.append(sm)
                elif state == "max retry":
                    self.retry_failed_requests.append(sm)
                else:
                    print("Unrecognized sm state: %s" % sm.state)
                    raise RuntimeError()
            else:
                print("Unrecognized sm status: %s" % sm.status)
                raise RuntimeError()

        self.direct_successful_queries = []
        self.direct_nvh_queries = []
        self.direct_rtf_queries = []
        self.retry_successful_queries = []
        self.retry_nvh_queries = []
        self.retry_retried_queries = []

        self._max_attempt_queries = []
        self._api_failed_queries = []

        def _insert_q(queries, query):
            if not query:
                print("Error, empty query!")
                raise RuntimeError()
            if queries:
                prv = queries[-1]
                if len(prv) != len(query):
                    print("Error, query length mismatch!")
                    print("  >> Last:")
                    for lg in prv:
                        print(lg)
                    print("  >> Curr:")
                    for lg in query:
                        print(lg)
                    raise RuntimeError()
            queries.append(query)

        def _insert_qs(queries, query_list):
            for query in query_list:
                _insert_q(queries, query) 

        def _assert_none(sm, attr):
            val = getattr(sm, attr, "None exist!")
            if val:
                print("Error, %s attr %s is not None: %s" 
                      % (sm.uuid, attr, val))
                raise RuntimeError()

        def _assert_retry(sm):
            count = 0
            if sm.rqs_final:
                count += 1
            if sm.cqs:
                count += 1
            if sm.rqs_retried_list:
                count += len(sm.rqs_retried_list)
            if sm.retries != count:
                print("Error, %s retry %d is not expected: %d"
                      % (sm.uuid, sm.retries, count))
                raise RuntimeError()

        def _assert_status(sm, status):
            if sm.status is not status:
                print("Error, %s status %s is not expected: %s"
                      % (sm.uuid, sm.status, status))
                raise RuntimeError()

        self._api_ret_intervals = []
        self._c_con_intervals = []

        def _set_interval(intervals, query,
                          l, l_key, l_service,
                          r, r_key, r_service):
            left = query[l]
            if l_key not in left.action:
                print("Error, action doesn't match: %s, %s"
                      % (l_key, left.action))
                raise RuntimeError()
            if l_service != left.service:
                print("Error, action service doesn't match!")
                raise RuntimeError()
            right = query[r]
            if r_key not in right.action:
                print("Error, action doesn't match: %s, %s"
                      % (r_key, right.action))
                raise RuntimeError()
            if r_service != right.service:
                print("Error, action service doesn't match!")
                raise RuntimeError()
            intervals.append((left.seconds, right.seconds,
                              right.seconds-left.seconds))
            return right.seconds-left.seconds

        for sm in state_machines.values():
            if sm.cqs_state in ["success",
                                "compute failed",
                                "compute finished failed"]:
                _insert_q(self.direct_successful_queries, sm.cqs)
                _assert_none(sm, "rqs_retried_list")
                _assert_none(sm, "rqs_final")
                _assert_none(sm, "rqs_state")
                _assert_status(sm, StateMachine.SUCCESS)
            elif sm.cqs_state == "no valid host":
                _insert_q(self.direct_nvh_queries, sm.cqs)
                _assert_none(sm, "rqs_retried_list")
                _assert_none(sm, "rqs_final")
                _assert_none(sm, "rqs_state")
                _assert_status(sm, StateMachine.SUCCESS)
            elif sm.cqs_state is None:
                if sm.rqs_state in ["success",
                                    "compute failed",
                                    "compute finished failed"]:
                    _insert_q(self.direct_rtf_queries, sm.cqs)
                    _insert_q(self.retry_successful_queries, sm.rqs_final)
                    _insert_qs(self.retry_retried_queries, sm.rqs_retried_list)
                    _assert_status(sm, StateMachine.SUCCESS)
                elif sm.rqs_state == "no valid host":
                    _insert_q(self.direct_rtf_queries, sm.cqs)
                    _insert_q(self.retry_nvh_queries, sm.rqs_final)
                    _insert_qs(self.retry_retried_queries, sm.rqs_retried_list)
                    _assert_status(sm, StateMachine.SUCCESS)
                elif sm.rqs_state == "max retry":
                    _insert_q(self.direct_rtf_queries, sm.cqs)
                    _insert_q(self._max_attempt_queries, sm.rqs_final)
                    _insert_qs(self.retry_retried_queries, sm.rqs_retried_list)
                    _assert_status(sm, StateMachine.SUCCESS)
                else:
                    if sm.status == StateMachine.FAILED:
                        _insert_q(self._api_failed_queries, sm.error_logs)
                        _assert_none(sm, "rqs_retried_list")
                        _assert_none(sm, "rqs_final")
                        _assert_none(sm, "rqs_state")
                        sm.retries -= 1
                    elif sm.status in [StateMachine.INCOMPLETE,
                                       StateMachine.ERROR]:
                        if self.cqs:
                            _insert_q(self.direct_rtf_queries, sm.cqs)
                        if self.rqs_retried_list:
                            _insert_qs(self.retry_retried_queries,
                                       sm.rqs_retried_list)
                        _assert_none(sm, "rqs_final")
                        _assert_none(sm, "rqs_state")
                        sm.retries -= 1
                    else:
                        print("Unrecognized state machine: %s" % sm.uuid)
                        raise RuntimeError()
            else:
                print("Unrecognized cps state: %s" % sm.cps_state)
                raise RuntimeError()
            _assert_retry(sm)

            if sm.cqs:
                _queries = []
                _queries.append(sm.cqs[0])
                _queries.append(sm.extra_logs[0])
                _set_interval(self._api_ret_intervals, _queries,
                              0, "received", "api",
                              1, "api returned", "api")

            if sm.rqs_final:
                _queries = []
                _queries.append(sm.cqs[-1])
                for qs in sm.rqs_retried_list:
                    _queries.append(qs[0])
                    _queries.append(qs[-1])
                _queries.append(sm.rqs_final[0])

                for i in range(0, len(_queries), 2):
                    _set_interval(self._c_con_intervals, _queries,
                                  i, "sent/retried", "compute",
                                  i+1, "received", "conductor")

        self.all_queries = list(chain(self.direct_successful_queries,
            self.direct_nvh_queries,
            self.direct_rtf_queries,
            self.retry_successful_queries,
            self.retry_nvh_queries,
            self.retry_retried_queries))

        for query in self.all_queries:
            prev = None
            for lg in query:
                if prev is not None:
                    if lg.seconds < prev.seconds:
                        print("Error, time inconsistend in %s!" % query.uuid)
                        print(" prev: %s" % lg)
                        print(" curr: %s" % lg)
                        raise RuntimeError()

        start_time = min([query[0].seconds for query in self.all_queries])
        end_time = max([query[-1].seconds for query in self.all_queries])
        self.wall_clock_total = end_time - start_time

        _successful_lapse = []
        for q in self.direct_successful_queries:
            _successful_lapse.append(q[-1].seconds - q[0].seconds)
        self.wall_clock_query = sum(_successful_lapse) / float(len(_successful_lapse))

        """ log sampling
        print_query(self.direct_successful_queries[0])
        print_query(self.direct_nvh_queries[0])
        print_query(self.direct_rtf_queries[0])
        print_query(self.retry_successful_queries[0])
        print_query(self.retry_nvh_queries[0])
        print_query(self.retry_retried_queries[0])
        print_query(self._api_failed_queries[0])
        print_query(self._max_attempt_queries[0])
        """

        # self._api_ret_intervals = []
        self._api_fail_intervals = []
        self._compu_intervals = []
        self._sched_intervals = []
        self._cond1_intervals = []
        self._cond2_intervals = []
        self._cond_ma_intervals = []
        self._cond_complete_intervals = []
        # self._c_con_intervals = []

        self._retry_intervals = []

        self._direct_inapi_intervals = []
        self._direct_a_con_intervals = []
        self._direct_cond1_intervals = []
        self._direct_c_sch_intervals = []
        self._direct_sched_intervals = []
        self._direct_s_con_intervals = []
        self._direct_cond2_intervals = []
        self._direct_c_com_intervals = []
        self._direct_compu_intervals = []

        self._direct_schedb_intervals = []
        self._direct_filter_intervals = []
        self._direct_gap_intervals = []

        for query in self._max_attempt_queries:
            _set_interval(self._cond_ma_intervals, query,
                          0, "received", "conductor",
                          1, "failed: attempt", "conductor")

        for query in self._api_failed_queries:
            _set_interval(self._api_fail_intervals, query,
                          0, "received", "api",
                          1, "failed:", "api")

        c_offset=0
        if C_SCHEDULER:
            c_offset=-2

        for query in self.direct_successful_queries:
            _set_interval(self._cond_complete_intervals, query,
                          2, "received", "conductor",
                          12+c_offset, "sent", "conductor")
            _set_interval(self._cond1_intervals, query,
                          2, "received", "conductor",
                          4, "sent scheduler", "conductor")
            _set_interval(self._sched_intervals, query,
                          5, "received", "scheduler",
                          10+c_offset, "selected", "scheduler")
            _set_interval(self._cond2_intervals, query,
                          11+c_offset, "decided", "conductor",
                          12+c_offset, "sent", "conductor")
            _set_interval(self._compu_intervals, query,
                          13+c_offset, "received", "compute",
                          14+c_offset, "success", "compute")

            if not C_SCHEDULER:
                _set_interval(self._direct_schedb_intervals, query,
                          7, "start_db", "scheduler",
                          8, "finish_db", "scheduler")
                _set_interval(self._direct_gap_intervals, query,
                          8, "finish_db", "scheduler",
                          14, "success", "compute")
            _set_interval(self._direct_filter_intervals, query,
                          6, "start scheduling", "scheduler",
                          9+c_offset, "finish scheduling", "scheduler")

            _set_interval(self._direct_inapi_intervals, query,
                          0, "received", "api",
                          1, "sent/retried", "api")
            _set_interval(self._direct_a_con_intervals, query,
                          1, "sent/retried", "api",
                          2, "received", "conductor")
            _set_interval(self._direct_cond1_intervals, query,
                          2, "received", "conductor",
                          4, "sent scheduler", "conductor")
            _set_interval(self._direct_c_sch_intervals, query,
                          4, "sent scheduler", "conductor",
                          5, "received", "scheduler")
            _set_interval(self._direct_sched_intervals, query,
                          5, "received", "scheduler",
                          10+c_offset, "selected", "scheduler")
            _set_interval(self._direct_s_con_intervals, query,
                          10+c_offset, "selected", "scheduler",
                          11+c_offset, "decided", "conductor")
            _set_interval(self._direct_cond2_intervals, query,
                          11+c_offset, "decided", "conductor",
                          12+c_offset, "sent", "conductor")
            _set_interval(self._direct_c_com_intervals, query,
                          12+c_offset, "sent", "conductor",
                          13+c_offset, "received", "compute")
            _set_interval(self._direct_compu_intervals, query,
                          13+c_offset, "received", "compute",
                          14+c_offset, "success", "compute")

        for query in self.direct_nvh_queries:
            _set_interval(self._cond1_intervals, query,
                          2, "received", "conductor",
                          4, "sent scheduler", "conductor")
            _set_interval(self._sched_intervals, query,
                          5, "received", "scheduler",
                          10+c_offset, "failed: novalidhost", "scheduler")
            _set_interval(self._cond_complete_intervals, query,
                          2, "received", "conductor",
                          11+c_offset, "failed:", "conductor")

            if not C_SCHEDULER:
                _set_interval(self._direct_schedb_intervals, query,
                          7, "start_db", "scheduler",
                          8, "finish_db", "scheduler")
            _set_interval(self._direct_filter_intervals, query,
                          6, "start scheduling", "scheduler",
                          9+c_offset, "finish scheduling", "scheduler")

            _set_interval(self._direct_inapi_intervals, query,
                          0, "received", "api",
                          1, "sent/retried", "api")
            _set_interval(self._direct_a_con_intervals, query,
                          1, "sent/retried", "api",
                          2, "received", "conductor")
            _set_interval(self._direct_cond1_intervals, query,
                          2, "received", "conductor",
                          4, "sent scheduler", "conductor")
            _set_interval(self._direct_c_sch_intervals, query,
                          4, "sent scheduler", "conductor",
                          5, "received", "scheduler")
            _set_interval(self._direct_sched_intervals, query,
                          5, "received", "scheduler",
                          10+c_offset, "failed: novalidhost", "scheduler")
            _set_interval(self._direct_s_con_intervals, query,
                          10+c_offset, "failed: novalidhost", "scheduler",
                          11+c_offset, "failed:", "conductor")

        for query in self.direct_rtf_queries:
            _set_interval(self._cond_complete_intervals, query,
                          2, "received", "conductor",
                          12+c_offset, "sent", "conductor")
            _set_interval(self._cond1_intervals, query,
                          2, "received", "conductor",
                          4, "sent scheduler", "conductor")
            _set_interval(self._sched_intervals, query,
                          5, "received", "scheduler",
                          10+c_offset, "selected", "scheduler")
            _set_interval(self._cond2_intervals, query,
                          11+c_offset, "decided", "conductor",
                          12+c_offset, "sent", "conductor")
            _set_interval(self._compu_intervals, query,
                          13+c_offset, "received", "compute",
                          14+c_offset, "fail: retry", "compute")

            if not C_SCHEDULER:
                _set_interval(self._direct_schedb_intervals, query,
                          7, "start_db", "scheduler",
                          8, "finish_db", "scheduler")
                _set_interval(self._direct_gap_intervals, query,
                          8, "finish_db", "scheduler",
                          14, "fail: retry", "compute")
            _set_interval(self._direct_filter_intervals, query,
                          6, "start scheduling", "scheduler",
                          9+c_offset, "finish scheduling", "scheduler")

            _set_interval(self._direct_inapi_intervals, query,
                          0, "received", "api",
                          1, "sent/retried", "api")
            _set_interval(self._direct_a_con_intervals, query,
                          1, "sent/retried", "api",
                          2, "received", "conductor")
            _set_interval(self._direct_cond1_intervals, query,
                          2, "received", "conductor",
                          4, "sent scheduler", "conductor")
            _set_interval(self._direct_c_sch_intervals, query,
                          4, "sent scheduler", "conductor",
                          5, "received", "scheduler")
            _set_interval(self._direct_sched_intervals, query,
                          5, "received", "scheduler",
                          10+c_offset, "selected", "scheduler")
            _set_interval(self._direct_s_con_intervals, query,
                          10+c_offset, "selected", "scheduler",
                          11+c_offset, "decided", "conductor")
            _set_interval(self._direct_cond2_intervals, query,
                          11+c_offset, "decided", "conductor",
                          12+c_offset, "sent", "conductor")
            _set_interval(self._direct_c_com_intervals, query,
                          12+c_offset, "sent", "conductor",
                          13+c_offset, "received", "compute")
            _set_interval(self._direct_compu_intervals, query,
                          13+c_offset, "received", "compute",
                          14+c_offset, "fail: retry", "compute")

        for query in self.retry_successful_queries:
            _set_interval(self._cond_complete_intervals, query,
                          0, "received", "conductor",
                          10+c_offset, "sent", "conductor")
            _set_interval(self._cond1_intervals, query,
                          0, "received", "conductor",
                          2, "sent scheduler", "conductor")
            _set_interval(self._cond2_intervals, query,
                          9+c_offset, "decided", "conductor",
                          10+c_offset, "sent", "conductor")
            _set_interval(self._sched_intervals, query,
                          3, "received", "scheduler",
                          8+c_offset, "selected", "scheduler")
            _set_interval(self._compu_intervals, query,
                          11+c_offset, "received", "compute",
                          12+c_offset, "success", "compute")

            _set_interval(self._retry_intervals, query,
                          0, "received", "conductor",
                          12+c_offset, "success", "compute")
            """
            _set_interval(self._schedb_intervals, query,
                          5, "start_db", "scheduler",
                          6, "finish_db", "scheduler")
            _set_interval(self._filter_intervals, query,
                          4, "start scheduling", "scheduler",
                          7, "finish scheduling", "scheduler")
            _set_interval(self._gap_intervals, query,
                          6, "finish_db", "scheduler",
                          12, "success", "compute")
            _set_interval(self._c_sch_intervals, query,
                          2, "sent scheduler", "conductor",
                          3, "received", "scheduler")
            _set_interval(self._s_con_intervals, query,
                          8, "selected", "scheduler",
                          9, "decided", "conductor")
            _set_interval(self._c_com_intervals, query,
                          10, "sent", "conductor",
                          11, "received", "compute")
            """

        for query in self.retry_nvh_queries:
            _set_interval(self._cond1_intervals, query,
                          0, "received", "conductor",
                          2, "sent scheduler", "conductor")
            _set_interval(self._cond_complete_intervals, query,
                          0, "received", "conductor",
                          9+c_offset, "failed:", "conductor")
            _set_interval(self._sched_intervals, query,
                          3, "received", "scheduler",
                          8+c_offset, "failed: novalidhost", "scheduler")

            _set_interval(self._retry_intervals, query,
                          0, "received", "conductor",
                          9+c_offset, "failed:", "conductor")
            """
            _set_interval(self._schedb_intervals, query,
                          5, "start_db", "scheduler",
                          6, "finish_db", "scheduler")
            _set_interval(self._filter_intervals, query,
                          4, "start scheduling", "scheduler",
                          7, "finish scheduling", "scheduler")
            _set_interval(self._c_sch_intervals, query,
                          2, "sent scheduler", "conductor",
                          3, "received", "scheduler")
            _set_interval(self._s_con_intervals, query,
                          8, "failed: novalidhost", "scheduler",
                          9, "failed:", "conductor")
            """

        for query in self.retry_retried_queries:
            _set_interval(self._cond1_intervals, query,
                          0, "received", "conductor",
                          2, "sent scheduler", "conductor")
            _set_interval(self._sched_intervals, query,
                          3, "received", "scheduler",
                          8+c_offset, "selected", "scheduler")
            _set_interval(self._cond2_intervals, query,
                          9+c_offset, "decided", "conductor",
                          10+c_offset, "sent", "conductor")
            _set_interval(self._compu_intervals, query,
                          11+c_offset, "received", "compute",
                          12+c_offset, "fail: retry", "compute")
            _set_interval(self._cond_complete_intervals, query,
                          0, "received", "conductor",
                          10+c_offset, "sent", "conductor")

            _set_interval(self._retry_intervals, query,
                          0, "received", "conductor",
                          12+c_offset, "fail: retry", "compute")
            """
            _set_interval(self._schedb_intervals, query,
                          5, "start_db", "scheduler",
                          6, "finish_db", "scheduler")
            _set_interval(self._filter_intervals, query,
                          4, "start scheduling", "scheduler",
                          7, "finish scheduling", "scheduler")
            _set_interval(self._gap_intervals, query,
                          6, "finish_db", "scheduler",
                          12, "fail: retry", "compute")
            _set_interval(self._c_sch_intervals, query,
                          2, "sent scheduler", "conductor",
                          3, "received", "scheduler")
            _set_interval(self._s_con_intervals, query,
                          8, "selected", "scheduler",
                          9, "decided", "conductor")
            _set_interval(self._c_com_intervals, query,
                          10, "sent", "conductor",
                          11, "received", "compute")
            """

        self.intervals_api_complete = Intervals(
            list(chain(self._api_ret_intervals,
                       self._api_fail_intervals)))
        self.intervals_sched = Intervals(
            self._sched_intervals)
        self.intervals_conductor = Intervals(
            list(chain(self._cond1_intervals,
                       self._cond2_intervals,
                       self._cond_ma_intervals)))
        self.intervals_conductor1 = Intervals(
            list(chain(self._cond_complete_intervals,
                       self._cond_ma_intervals)))
        self.intervals_compute = Intervals(
            self._compu_intervals)

        self.intervals_direct_inapi = Intervals(
            self._direct_inapi_intervals)
        self.intervals_direct_a_con = Intervals(
            self._direct_a_con_intervals)
        self.intervals_direct_cond1 = Intervals(
            self._direct_cond1_intervals)
        self.intervals_direct_c_sch = Intervals(
            self._direct_c_sch_intervals)
        self.intervals_direct_sched = Intervals(
            self._direct_sched_intervals)
        self.intervals_direct_s_con = Intervals(
            self._direct_s_con_intervals)
        self.intervals_direct_cond2 = Intervals(
            self._direct_cond2_intervals)
        self.intervals_direct_c_com = Intervals(
            self._direct_c_com_intervals)
        self.intervals_direct_compute = Intervals(
            self._direct_compu_intervals)

        self.intervals_direct_cache_refresh = Intervals(
            self._direct_schedb_intervals)
        self.intervals_direct_filter = Intervals(
            self._direct_filter_intervals)
        self.intervals_direct_gap = Intervals(
            self._direct_gap_intervals)

        self.intervals_api_fail = Intervals(
            self._api_fail_intervals)
        self.intervals_retry = Intervals(
            list(chain(self._retry_intervals,
                       self._c_con_intervals)))


class Intervals(object):
    def __init__(self, intervals):
        self.intervals = intervals
        self.intervals.sort(key=lambda tup: (tup[0], tup[1]))

    def wall_time(self):
        total = 0
        start = None
        end = None
        for inte in self.intervals:
            if start is None:
                start = inte[0]
                end = inte[1]
            elif inte[0] <= end:
                end = max(end, inte[1])
            else:
                total += (end-start)
                start = inte[0]
                end = inte[1]
        if start is not None:
            total += (end-start)
        return total

    def average(self):
        if not self.intervals:
            return 0
        return (sum([interval[2] for interval in self.intervals])
                / len(self.intervals))

    def x_y_incremental(self):
        data0 = [(interval[0], 1) for interval in self.intervals]
        data0.extend([(interval[1], -1) for interval in self.intervals])
        data0.sort(key=lambda tup: tup[0])

        x_list = []
        y_list = []
        y = 0
        x = None
        for data in data0:
            if x is None:
                x = data[0]
            elif x != data[0]:
                x_list.append(x)
                y_list.append(y)
                x = data[0]
            y += data[1]
        if x is not None:
            x_list.append(x)
            y_list.append(y)

        return x_list, y_list

    def x_y_lasts(self):
        x_list = [interval[0] for interval in self.intervals]
        y_list = [interval[2] for interval in self.intervals]
        return x_list, y_list
        
    def print_all(self):
        print("Intervals:")
        for inte in self.intervals:
            print(inte)


def print_query(query):
    print("")
    for lg in query:
        print(lg)


def generate_report(args, state_machines, schedulers, computes):
    ## number_of_schedulers
    number_of_active_schedulers = len(schedulers)
    ## number_of_computes
    number_of_active_computes = len(computes)
    ## number_of_api_workers
    ## number_of_conductor_workers
    ## number_of_max_retries

    sm_parser = StateMachinesParser(state_machines)
    count_total_requests = len(sm_parser.total_requests)
    count_successful_requests = len(sm_parser.success_requests)
    count_nvh_requests = len(sm_parser.nvh_requests)
    count_retry_failed_requests = len(sm_parser.retry_failed_requests)
    count_api_failed_requests = len(sm_parser.api_failed_requests)
    count_incomplete_requests = len(sm_parser.incomplete_requests)
    count_error_requests = len(sm_parser.error_requests)

    count_total_queries = len(sm_parser.all_queries)
    count_direct_successful_queries = len(sm_parser.direct_successful_queries)
    count_direct_nvh_queries = len(sm_parser.direct_nvh_queries)
    count_direct_retried_queries = len(sm_parser.direct_rtf_queries)
    count_retried_successful_queries = len(sm_parser.retry_successful_queries)
    count_retried_nvh_queries = len(sm_parser.retry_nvh_queries)
    count_retried_retried_queries = len(sm_parser.retry_retried_queries)

    time_wall_clock_total = sm_parser.wall_clock_total
    time_wall_clock_api = sm_parser.intervals_api_complete.wall_time()
    time_wall_clock_conductor = sm_parser.intervals_conductor.wall_time()
    time_wall_clock_scheduler = sm_parser.intervals_sched.wall_time()
    time_wall_clock_compute = sm_parser.intervals_compute.wall_time()
    time_wall_clock_query = sm_parser.wall_clock_query 

    time_query_inapi_avg = sm_parser.intervals_direct_inapi.average()
    time_query_a_con_avg = sm_parser.intervals_direct_a_con.average()
    time_query_cond1_avg = sm_parser.intervals_direct_cond1.average()
    time_query_c_sch_avg = sm_parser.intervals_direct_c_sch.average()
    time_query_sched_avg = sm_parser.intervals_direct_sched.average()
    time_query_s_con_avg = sm_parser.intervals_direct_s_con.average()
    time_query_cond2_avg = sm_parser.intervals_direct_cond2.average()
    time_query_c_com_avg = sm_parser.intervals_direct_c_com.average()
    time_query_compu_avg = sm_parser.intervals_direct_compute.average()

    time_query_filter_avg = sm_parser.intervals_direct_filter.average()
    time_query_cache_avg = sm_parser.intervals_direct_cache_refresh.average()
    time_query_gap_avg = sm_parser.intervals_direct_gap.average()

    sum_complete_query = time_query_inapi_avg + \
                         time_query_a_con_avg + \
                         time_query_cond1_avg + \
                         time_query_c_sch_avg + \
                         time_query_sched_avg + \
                         time_query_s_con_avg + \
                         time_query_cond2_avg + \
                         time_query_c_com_avg + \
                         time_query_compu_avg
    percent_inapi = time_query_inapi_avg / sum_complete_query * 100
    percent_msg = (time_query_a_con_avg
                   + time_query_c_sch_avg
                   + time_query_s_con_avg
                   + time_query_c_com_avg) / sum_complete_query * 100
    percent_cond = (time_query_cond1_avg
                    + time_query_cond2_avg) / sum_complete_query * 100
    percent_sched = time_query_sched_avg / sum_complete_query * 100
    percent_compu = time_query_compu_avg / sum_complete_query * 100

    percent_sched_filter = time_query_filter_avg / sum_complete_query * 100
    percent_sched_cache = time_query_cache_avg / sum_complete_query * 100
    percent_sched_gap = time_query_gap_avg / sum_complete_query * 100

    request_per_sec = (count_total_requests - count_api_failed_requests) \
        / time_wall_clock_total
    query_per_sec = count_total_queries / time_wall_clock_total
    success_per_sec = count_successful_requests / time_wall_clock_total

    query_retry_percent = (count_retried_retried_queries
                           + count_direct_retried_queries) \
        / float(count_successful_requests) * 100
    request_failure_percent = count_api_failed_requests \
        / float(count_total_requests) * 100

    if args.outfile is None:
        print("")
        print(" >> FINAL REPORT:")
        print("active schedulers:         %d" % number_of_active_schedulers)
        print("active computes:           %d" % number_of_active_computes)
        print("")
        print("total requests:            %d" % count_total_requests)
        print("successful requests:       %d" % count_successful_requests)
        print("nvh requests:              %d" % count_nvh_requests)
        print("rtf requests:              %d" % count_retry_failed_requests)
        print("api failed requests:       %d" % count_api_failed_requests)
        print("incomplete requests:       %d" % count_incomplete_requests)
        print("error requests:            %d" % count_error_requests)
        print("")
        print("total valid queries:       %d" % count_total_queries)
        print("direct successful queries: %d" % count_direct_successful_queries)
        print("direct nvh queries:        %d" % count_direct_nvh_queries)
        print("direct retried queries:    %d" % count_direct_retried_queries)
        print("retry successful queries:  %d" % count_retried_successful_queries)
        print("retry nvh queries:         %d" % count_retried_nvh_queries)
        print("retry retried queires:     %d" % count_retried_retried_queries)
        print("")
        print("wall clock total(s):       %7.5f" % time_wall_clock_total)
        print("wall clock api:            %7.5f" % time_wall_clock_api)
        print("wall clock conductor:      %7.5f" % time_wall_clock_conductor)
        print("wall clock scheduler:      %7.5f" % time_wall_clock_scheduler)
        print("wall clock compute:        %7.5f" % time_wall_clock_compute)
        print("wall clock query:          %7.5f" % time_wall_clock_query)
        print("")
        print("time inapi avg:            %7.5f" % time_query_inapi_avg)
        print("time a-con avg:            %7.5f" % time_query_a_con_avg)
        print("time cond1 avg:            %7.5f" % time_query_cond1_avg)
        print("time c-sch avg:            %7.5f" % time_query_c_sch_avg)
        print("time sched avg:            %7.5f" % time_query_sched_avg)
        print("time s-con avg:            %7.5f" % time_query_s_con_avg)
        print("time cond2 avg:            %7.5f" % time_query_cond2_avg)
        print("time c-com avg:            %7.5f" % time_query_c_com_avg)
        print("time compu avg:            %7.5f" % time_query_compu_avg)
        print("")
        print("time filter avg:           %7.5f" % time_query_filter_avg)
        print("time cache refresh avg:    %7.5f" % time_query_cache_avg)
        print("time gap avg:              %7.5f" % time_query_gap_avg)
        print("")
        print("percent api part:          %7.5f" % percent_inapi)
        print("percent msg part:          %7.5f" % percent_msg)
        print("percent cond part:         %7.5f" % percent_cond)
        print("percent sch part:          %7.5f" % percent_sched)
        print("percent compute part:      %7.5f" % percent_compu)
        print("percent filter part:       %7.5f" % percent_sched_filter)
        print("percent cache-refresh part:%7.5f" % percent_sched_cache)
        print("percent gap part:          %7.5f" % percent_sched_gap)
        print("")
        print("request per sec:           %7.5f" % request_per_sec)
        print("query per sec:             %7.5f" % query_per_sec)
        print("success per sec:           %7.5f" % success_per_sec)
        print("")
        print("percent query retry:       %7.5f" % query_retry_percent)
        print("percent request api fail:  %7.5f" % request_failure_percent)
    else:
        outfile = open(args.outfile, 'a+')
        if args.csv_print_header:
            header_fields = [
                "Name",
                "Active schedulers",
                "Active computes",
                "Total requests",
                "Successful requests",
                "Nvh requests",
                "Rtf requests",
                "Api failed requests",
                "Incomplete requests",
                "Error requests",
                "Total valid queries",
                "Direct successful queries",
                "Direct nvh queries",
                "Direct retried queries",
                "Retry successful queries",
                "Retry nvh queries",
                "Retry retried queires",
                "Clock total(s)",
                "Clock api",
                "Clock conductor",
                "Clock scheduler",
                "Clock compute",
                "Clock query",
                "Time inapi avg",
                "Time a-con avg",
                "Time cond1 avg",
                "Time c-sch avg",
                "Time sched avg",
                "Time s-con avg",
                "Time cond2 avg",
                "Time c-com avg",
                "Time compu avg",
                "Time filter avg",
                "Time cache refresh avg",
                "Time gap avg",
                "Percent api part",
                "Percent msg part",
                "Percent cond part",
                "Percent sch part",
                "Percent compute part",
                "Percent filter part",
                "Percent cache-refresh part",
                "Percent gap part",
                "Request per sec",
                "Query per sec",
                "Success per sec",
                "Percent query retry",
                "Percent request api fail",
            ]
            outfile.write(','.join(header_fields) + '\n')
        row_fields = [
            args.folder,
            number_of_active_schedulers,
            number_of_active_computes,
            count_total_requests,
            count_successful_requests,
            count_nvh_requests,
            count_retry_failed_requests,
            count_api_failed_requests,
            count_incomplete_requests,
            count_error_requests,
            count_total_queries,
            count_direct_successful_queries,
            count_direct_nvh_queries,
            count_direct_retried_queries,
            count_retried_successful_queries,
            count_retried_nvh_queries,
            count_retried_retried_queries,
            time_wall_clock_total,
            time_wall_clock_api,
            time_wall_clock_conductor,
            time_wall_clock_scheduler,
            time_wall_clock_compute,
            time_wall_clock_query,
            time_query_inapi_avg,
            time_query_a_con_avg,
            time_query_cond1_avg,
            time_query_c_sch_avg,
            time_query_sched_avg,
            time_query_s_con_avg,
            time_query_cond2_avg,
            time_query_c_com_avg,
            time_query_compu_avg,
            time_query_filter_avg,
            time_query_cache_avg,
            time_query_gap_avg,
            percent_inapi,
            percent_msg,
            percent_cond,
            percent_sched,
            percent_compu,
            percent_sched_filter,
            percent_sched_cache,
            percent_sched_gap,
            request_per_sec,
            query_per_sec,
            success_per_sec,
            query_retry_percent,
            request_failure_percent,
        ]
        outfile.write(','.join(str(f) for f in row_fields) + '\n')
        outfile.flush()
        outfile.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder',
                        default=".",
                        help="The logs are in that folder.")
    parser.add_argument('--brief',
                        action="store_true",
                        help="Supress verbose error report.")
    parser.add_argument('--csv-print-header', action="store_true",
                        help="Write a row into the CSV file for the headers.")
    parser.add_argument('--outfile',
                        help="The output file of report.")
    parser.add_argument('--offset',
                        type=float,
                        default=0,
                        help="The compute nodes are remote.")
    args = parser.parse_args()
    current_path = path.dirname(os.path.realpath(__file__))
    current_path = path.join(current_path, args.folder)

    log_lines = []
    relation = {}

    for f in os.listdir(current_path):
        f_dir = path.join(current_path, f)
        if not path.isfile(f_dir):
            continue
        if not f.endswith(".log"):
            continue
        if f == "out.log":
            continue
        pieces = f.split('-')

        service = pieces[1]
        host = pieces[2].split('.')[0]

        with open(f_dir, 'r') as reader:
            for line in reader:
                if "BENCH-" not in line:
                    continue
                if "Bench initiated!" in line:
                    continue
                lg = LogLine(line, service, host, f, args.offset)
                if not lg.get_relation(relation):
                    print("Fail getting relation!")
                    return
                log_lines.append(lg)

    relation_name = {}
    for rel in relation.values():
        rel_record = relation_name.get(rel.instance_name, None)
        if rel_record:
            print("Fail, relation has duplicated name! %s, %s"
                  % (rel_record.get_line(), rel.get_line()))
            return
        relation_name[rel.instance_name] = rel

    for i in range(0, len(log_lines)):
        try:
            if not log_lines[i].apply(relation, relation_name, log_lines, i):
                print("Parsing not ready: wait for complete relations.")
                return
        except KeyError as e:
            if not args.brief:
                print("Warning(%s): %s" % (e, log_lines[i]))
    log_lines.sort(key=lambda item: item.seconds)

    with open(path.join(current_path, "out.log"), "w") as writer:
        for lg in log_lines:
            print(lg, file=writer)

    group_by_service = collections.defaultdict(list)
    group_by_instance = collections.defaultdict(list)
    compute_services = set()
    scheduler_services = set()

    for lg in log_lines:
        group_by_service[lg.service].append(lg)
        group_by_instance[lg.instance_name].append(lg)
        if lg.service == "compute":
            compute_services.add(lg.host)
        elif lg.service == "scheduler":
            scheduler_services.add(lg.host)

    group_by_state_machine = {}
    status_count = collections.defaultdict(int)
    for name in group_by_instance.keys():
        state_machine = StateMachine(name, group_by_instance[name])
        state_machine.parse()
        group_by_state_machine[name] = state_machine
        status = state_machine.status
        if not args.brief:
            if status == StateMachine.ERROR:
                state_machine.report()
            elif status == StateMachine.INCOMPLETE:
                    print(" >> Instance %s still in step %s."
                          % (state_machine.uuid, state_machine.state))
            elif status == StateMachine.SUCCESS and state_machine.error_logs:
                state_machine.report()
        status_count[status] += 1

    print("")
    print(" >> LOG SUMMARY")
    print("Active schedulers: %d" % len(scheduler_services))
    print("Active computes: %d" % len(compute_services))
    print("Total requests count: %d" % sum(status_count.values()))
    for key in status_count.keys():
        print ("  %s requests: %d" % (key, status_count[key]))
    if args.brief:
        return

    generate_report(args,
                    group_by_state_machine,
                    scheduler_services,
                    compute_services)


if __name__ == "__main__":
    main()
