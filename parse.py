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
import os
from os import path
import sys


class LogLine(object):
    def __init__(self, line, log_file):
        # line
        self.line = line
        pieces = line.split()

        # service, host
        pieces7 = pieces[7].split('-')
        self.host = pieces7[2][:-1]
        self.service = pieces7[1]

        self.filename = log_file.name
        if log_file.host is None:
            log_file.host = self.host
        elif log_file.host != self.host:
            raise RuntimeError("Host and service mismatch in log %s"
                               % self.filename)

        if log_file.service is None:
            log_file.service = self.service
        elif log_file.service != self.service:
            raise RuntimeError("Host and service mismatch in log %s"
                               % self.filename)

        # seconds
        time_pieces = pieces[1].split(":")
        self.seconds = int(time_pieces[0]) * 3600 + \
            int(time_pieces[1]) * 60 + \
            float(time_pieces[2])
        
        # instance_id, instance_name
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

        self.time = pieces[1]
        self.request_id = pieces[4][5:]
        self.action = " ".join(pieces[9:])
        self.correct = True

    def __repr__(self):
        return str(self.seconds) + " " + \
            self.time + " " + \
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
                        raise RuntimeError("Cannot parse relation start_db!")
                else:
                    print("Unable to resolve instance relations1: %s!"
                          % self.get_line())
                    raise RuntimeError("Cannot parse relation start_db!")
            elif "finish_db" in self.action:
                if "finish scheduling" in log_lines[i+1].action:
                    self.instance_id = log_lines[i+1].instance_id
                    if self.instance_id == "?":
                        raise RuntimeError("Cannot parse relation finish_db!")
                else:
                    print("Unable to resolve instance relations2: %s!"
                          % self.get_line())
                    raise RuntimeError("Cannot parse relation finish_db!")
            else:
                print("Unable to resolve instance relations: %s!"
                      % self.get_line())
                raise RuntimeError("Cannot parse relation ?-?!")

        if self.instance_id == "?":
            rel = relation_name.get(self.instance_name, None)
            if not rel:
                return self.instance_name
            self.instance_id = rel.instance_id
            return True
        elif self.instance_name == "?":
            rel = relation_id.get(self.instance_id, None)
            if not rel:
                return self.instance_id
            self.instance_name = rel.instance_name
            return True
        else:
            rel = relation_id.get(self.instance_id, None)
            if not rel:
                return self.instance_id
            if self.instance_name != rel.instance_name:
                raise RuntimeError("Apply logline mismatch, rel: %s, log: %s"
                                   % (rel.line, self))
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


DIAGRAMS = collections.namedtuple("Diagrams",
                                  "api cond1 cond2 sched comp")


class Node(object):
    def __init__(self, p, id, end=False):
        self.id = id
        self.end = end
        self.edges = []
        self.p = p
        if id in p.nodes:
            raise RuntimeError("Graph already has node %s!" % id)
        p.nodes[id] = self

    def build_next(self, diagram, service, retried, keyword,
                   id=None, end=False, p=None, next=None):
        if self.end:
            raise RuntimeError("node %s is an end!" % self.id)

        if next is None:
            if id is None or p is None:
                raise RuntimeError("id and next are all none!")
            next = Node(p, id, end)
        edge = Edge(next, diagram, service, retried, keyword)
        self.edges.append(edge)
        return next

    def step(self, log, retries):
        # 1. stay
        if self.p.check_extra(log, retries):
            return self
        # 2. step
        for edge in self.edges:
            if edge.accept(log, retries):
                return edge
        # 3. error
        return None


class Edge(object):
    def __init__(self, node, diagram, service, retried, keyword):
        self.node = node
        self.diagram = diagram

        # constraints
        self.service = service
        self.retried = retried
        self.keyword = keyword

    def accept(self, log, retries):
        if retries < 1:
            return False
        if self.retried is True or\
           (self.retried is False and retries == 1) or\
           (self.retried is None and int(log.action.split(" ")[1]) == retries):
            return log.assert_c(self.service, self.keyword)
        return False


class Diagram(object):
    diagrams = DIAGRAMS(api="api",
                        cond1="cond1",
                        cond2="cond2",
                        sched="sched",
                        comp="comp")

    def __init__(self):
        self.start = None
        self.nodes = {}
        self.extra_edges = []

        self._build_diagram()

    def check_extra(self, log, retries):
        for edge in self.extra_edges:
            if edge.accept(log, retries):
                return True
        return False

    def _build(self, from_, diagram, service, retried, keyword, id, end=False):
        if id not in self.nodes:
            next = from_.build_next(diagram, service, retried, keyword,
                                    id, end, self)
        else:
            next = self.nodes[id]
            from_.build_next(diagram, service, retried, keyword, next=next)
            if next.end != end:
                raise RuntimeError("Node doesn't match!")
        return next

    def _build_diagram(self):
        diag = self.diagrams
        build = self._build

        node0  = Node(self, 0)
        node1  = build(node0,
                       diag.api, "api", False, "received",
                       1)
        node20 = build(node1, diag.api, "api", False, "failed:",
                       20, True)
        node2  = build(node1, diag.api, "api", False, "sent/retried",
                       2)
        node3  = build(node2, diag.cond1, "conductor", True, "received",
                       3)
        node21 = build(node3, diag.cond1, "conductor", True, "failed: attempt",
                       21, True)
        node4  = build(node3, diag.cond1, "conductor", None, "attempts",
                       4)
        node5  = build(node4, diag.cond1, "conductor", True, "sent scheduler",
                       5)
        node6  = build(node5, diag.sched, "scheduler", True, "received",
                       6)
        node7  = build(node6, diag.sched, "scheduler", True, "start scheduling",
                       7)
        node8  = build(node7, diag.sched, "scheduler", True, "start_db",
                       8)
        node9  = build(node8, diag.sched, "scheduler", True, "finish_db",
                       9)
        node10 = build(node9, diag.sched, "scheduler", True, "finish scheduling",
                       10)
        node11 = build(node10, diag.sched, "scheduler", True, "selected",
                       11)
        node12 = build(node10, diag.sched, "scheduler", True, "failed: novalidhost",
                       12)
        node22 = build(node12, diag.cond2, "conductor", True, "failed: novalidhost",
                       22, True)
        node13 = build(node11, diag.cond2, "conductor", True, "decided",
                       13)
        node14 = build(node13, diag.cond2, "conductor", True, "sent",
                       14)
        node15 = build(node14, diag.comp, "compute", True, "received",
                       15)
        node24 = build(node15, diag.comp, "compute", True, "success",
                       24)
        node25 = build(node24, diag.comp, "compute", True, "finished: active",
                       25, True)
        node16 = build(node15, diag.comp, "compute", True, "fail: retry",
                       16)
        node23 = build(node15, diag.comp, "compute", True, "fail:",
                       23, True)
        node2  = build(node16, diag.comp, "compute", True, "sent/retried",
                       2)

        self.extra_edges.extend([
            Edge(None, diag.api, "api", False, "api returned"),
            Edge(None, diag.comp, "compute", True, "finished: rescheduled")
        ])
        self.start = node0


class Pace(object):
    def __init__(self, id, node, edge, log, nxt, bfo):
        self.edge = edge
        self.node = node
        self.log = log
        self.id = id
        self.nxt = nxt
        self.bfo = bfo
        self.more = []

    @classmethod
    def new(cls, graph):
        return cls(0, graph.start, None, None, None, None)

    def step(self, log, retries):
        next = self.node.step(log, retries)
        if next is None:
            return None
        elif next is self.node:
            return self
        else:
            ret = Pace(self.id+1, next.node, next, log, None, self)
            self.nxt = ret
            return ret


class RequestStateMachine(object):
    NOTSTARTED = "NotStarted"
    INCOMPLETE = "Incomplete"
    ERROR = "Error"

    READY = "Ready"

    FAILED = "Failed"
    SUCCESS = "Success"
    MAXATTEMPT = "MaxAttempt"
    NOVALIDHOST = "NoValidHost"

    def __init__(self, uuid, logs, graph):
        self.graph = graph
        self.logs = logs
        self.uuid = uuid

        self.retries = None
        self.pace = None
        self.state = None
        self.state_msg = None
        self.error_logs = None
        self.extras = None
        self.paces = None

        self.more_logs = None

    def _parse_one(self, log):
        pace = self.pace.step(log, self.retries)
        if pace is None:
            # error
            return False
        elif pace is self.pace:
            # extra
            if log.service == "api":
                ps = pace
                while ps:
                    if ps.edge.diagram == self.graph.diagrams.api:
                        ps.more.append(log)
                        break
                    ps = ps.bfo
                if not ps:
                    raise RuntimeError("cannot place more")
            else:
                pace.more.append(log)
            self.extras.add(pace)
            return True
        else:
            # next
            if pace.id in self.paces:
                raise RuntimeError("Duplicated pace id %s" % pace.id)
            else:
                self.paces[pace.id] = pace
            self.pace = pace
            if pace.node.id == 16:
                self.retries += 1
            return True

    def _parse_controller_one(self, log):
        if self.pace.node.id == 14:
            pid = self.pace.id

            pace1 = Pace(pid+1, self.pace.node.edges[0].node,
                self.pace.node.edges[0], None, None, self.pace)
            pace2 = Pace(pid+2, pace1.node.edges[1].node,
                pace1.node.edges[0], None, None, pace1)
            pace3 = Pace(pid+3, pace2.node.edges[0].node,
                pace2.node.edges[0], None, None, pace2)

            self.paces[pace1.id] = pace1
            self.paces[pace2.id] = pace2
            self.paces[pace3.id] = pace3

            self.pace.nxt = pace1
            pace1.nxt = pace2
            pace2.nxt = pace3

            self.comp_mid_slots.append(self.pace.id)
            self.pace = pace3
            self.retries += 1
            if self.pace.node.id != 2:
                raise RuntimeError("The mid slot expected to end at 2, "
                                   "but at %s!" % self.pace.node.id)
        return self._parse_one(log)

    def parse_controller(self, controller_logs):
        self.retries = 1
        self.pace = Pace.new(self.graph)
        self.state = self.NOTSTARTED
        self.state_msg = "not started"
        self.error_logs = []
        self.extras = set()
        self.paces = {self.pace.id: self.pace}

        self.comp_mid_slots = []
        self.comp_end_slot = None

        self.logs = controller_logs

        for log in controller_logs:
            if self.pace.node.end is True:
                self.more_logs.append(log)

            elif self.state == self.NOTSTARTED:
                if not self._parse_controller_one(log):
                    self.state = self.ERROR
                    self.state_msg = ("error at node %s, the following edges "
                                      "doesn't match the log %s" %
                                      (self.pace.node.id, log))
                    self.error_logs.append(log)
            else:
                # error
                self.error_logs.append(log)

        # check the final state
        if self.state == self.ERROR:
            pass
        elif self.pace.node.id == 14:
            self.comp_end_slot = self.pace.id
            self.state = self.READY
            self.state_msg = "Ready to parse compute logs, attempt: %s" % self.retries
        elif self.pace.node.end is True:
            if self.pace.node.id == 20:
                self.state = self.FAILED
                self.state_msg = "api failed"
            elif self.pace.node.id == 21:
                if self.retries == 1:
                    self.state = self.FAILED
                    self.state_msg = "attempt 1 failed"
                else:
                    self.state = self.MAXATTEMPT
                    self.state_msg = "max attempt reached: %s" % self.retries
            elif self.pace.node.id == 22:
                self.state = self.NOVALIDHOST
                self.state_msg = "no valid host at attempt: %s" % self.retries
            else:
                self.state = self.ERROR
                self.state_msg = "unknown final state %s: %s" \
                    % (self.pace.node.id, self.pace.log)
        elif self.pace.node is not self.graph.start:
            self.state = self.INCOMPLETE
            self.state_msg = "incomplete log: %s" % self.pace.log

    def parse_computes(self, compute_logs):
        if self.state in [self.NOTSTARTED,
                          self.INCOMPLETE,
                          self.ERROR]:
            return
        for slot in self.comp_mid_slots:
            host = self.paces[slot].log.action.split(" ")[1]
            compute_log = compute_logs[host]
            logs = compute_log.logs_by_ins[self.uuid]

            if self.graph.nodes[14].edges[0].accept(logs[0], 1) and \
               self.graph.nodes[15].edges[1].accept(logs[1], 1) and \
               self.graph.nodes[16].edges[0].accept(logs[2], 1) and \
               self.graph.extra_edges[1].accept(logs[3], 1):
                self.paces[slot+1].log = logs[0]
                self.paces[slot+2].log = logs[1]
                self.paces[slot+3].log = logs[2]
                self.paces[slot+3].more.append(logs[3])
                self.extras.add(logs[3])
                compute_log.logs_by_ins[self.uuid] = logs[4:]
                lo = self.paces[slot].log.seconds-logs[0].seconds
                hi = self.paces[slot+4].log.seconds - logs[2].seconds
                if not compute_log.set_offset(lo, hi):
                    self.state = self.ERROR
                    self.state_msg = "compute log %s offset error: %s"\
                                     % (host, (lo, hi))
                    return
            else:
                self.state = self.ERROR
                self.state_msg = "compute log cannot fit to mid:\n %s" % logs
                return

        slot = self.comp_end_slot
        if slot is None:
            return

        host = self.paces[slot].log.action.split(" ")[1]
        compute_log = compute_logs[host]
        logs = compute_log.logs_by_ins[self.uuid]

        lo = self.paces[slot].log.seconds-logs[0].seconds
        if not compute_log.set_offset(lo, None):
            self.state = self.ERROR
            self.state_msg = "compute log %s offset error: %s"\
                             % (host, (lo, None))
            return

        for log in logs:
            if self.pace.node.end is True:
                self.more_logs.append(log)
            elif self.state == self.READY:
                if not self._parse_one(log):
                    self.state = self.ERROR
                    self.state_msg = ("error at node %s, the following edges "
                                      "doesn't match the log %s" %
                                      (self.pace.node.id, log))
                    self.error_logs.append(log)
            else:
                # error
                self.state = self.ERROR
                self.error_logs.append(log)
        if self.state == self.ERROR:
            pass
        elif self.state == self.READY:
            if self.pace.node.id == 23:
                self.state = self.FAILED
                self.state_msg = "compute node failed: %s" % self.pace.log
            elif self.pace.node.id == 25:
                self.state = self.SUCCESS
                self.state_msg = "success at attempt: %s" % self.retries
            else:
                self.state = self.ERROR
                self.state_msg = "unknown final state %s: %s" \
                    % (self.pace.node.id, self.pace.log)
        else:
            self.state_msg = "unknow state: %s" % self.state
            self.state = self.ERROR
            
    """
    def parse(self):
        self.retries = 1
        self.pace = Pace.new(self.graph)
        self.state = self.NOTSTARTED
        self.state_msg = "not started"
        self.error_logs = []
        self.extras = set()
        self.paces = {self.pace.id: self.pace}

        self.more_logs = []
        
        for log in self.logs:
            if self.pace.node.end is True:
                if self.pace.step(log, self.retries) == self.pace:
                    self.extras.add(self.pace)
                else:
                    self.more_logs.append(log)
            elif self.state == self.NOTSTARTED:
                if not self._parse_one(log):
                    self.state = self.ERROR
                    self.state_msg = ("error at node %s, the following edges "
                                      "doesn't match the log %s" %
                                      (self.pace.node.id, log))
                    self.error_logs.append(log)
            else:
                # error
                self.error_logs.append(log)

        # check the final state
        if self.state == self.ERROR:
            pass
        elif self.pace.node.end is True:
            if self.pace.node.id == 20:
                self.state = self.FAILED
                self.state_msg = "api failed"
            elif self.pace.node.id == 21:
                if self.retries == 1:
                    self.state = self.FAILED
                    self.state_msg = "attempt 1 failed"
                else:
                    self.state = self.MAXATTEMPT
                    self.state_msg = "max attempt reached: %s" % self.retries
            elif self.pace.node.id == 22:
                self.state = self.NOVALIDHOST
                self.state_msg = "no valid host at attempt: %s" % self.retries
            elif self.pace.node.id == 23:
                self.state = self.FAILED
                self.state_msg = "compute node failed: %s" % self.pace.log
            elif self.pace.node.id == 25:
                self.state = self.SUCCESS
                self.state_msg = "success at attempt: %s" % self.retries
            else:
                self.state = self.ERROR
                self.state_msg = "unknown final state %s: %s" \
                    % (self.pace.node.id, self.pace.log)
        elif self.pace.node is not self.graph.start:
            self.state = self.INCOMPLETE
            self.state_msg = "incomplete log: %s" % self.pace.log
    """

    def check(self):
        ps = self.paces[1]
        prv_log = ps.log
        prv = prv_log.seconds
        ps = ps.nxt
        while ps is not None:
            if prv > ps.log.seconds:
                self.state = self.ERROR
                self.state_msg = "time inconsistent: %s, %s"\
                                 % (prv_log, ps.log)
                return
            prv_log = ps.log
            if prv_log is None:
                self.state = self.ERROR
                self.state_msg = "empty log at step %s, node %s"\
                                 % (ps.id, ps.node.id)
                return
            if ps.node.id == 2:
                if len(ps.more) != 1 \
                        or ("api return" not in ps.more[0].action
                            and "resche" not in ps.more[0].action) \
                        or ps.more[0].seconds < ps.log.seconds:
                    self.state = self.ERROR
                    self.state_msg = "log more mismatch: %s; %s" \
                                     % (ps.log, ps.more)
                    return

            prv = prv_log.seconds
            ps = ps.nxt

    def get_intervals(self, targets, d_targets):
        ret_dict = collections.defaultdict(list)
        end_dict = collections.defaultdict(list)
        for target in targets:
            if target[0] not in self.graph.nodes.keys():
                raise RuntimeError("Node number %s cannot find in graph"
                                   % target[0])
            if target[1] not in self.graph.nodes.keys():
                raise RuntimeError("Node number %s cannot find in graph"
                                   % target[1])
            end_dict[target[1]].append(target)

        ret_d_dict = collections.defaultdict(list)
        end_d_dict = collections.defaultdict(list)
        for target in d_targets:
            if target[0] not in self.graph.nodes.keys():
                raise RuntimeError("Node number %s cannot find in graph"
                                   % target[0])
            if target[1] not in self.graph.nodes.keys():
                raise RuntimeError("Node number %s cannot find in graph"
                                   % target[1])
            end_d_dict[target[1]].append(target)

        tracking_dict = {}
        ps = self.paces[0]
        retries = 0

        r_from = None

        while ps:
            node_id = ps.node.id
            if node_id == 2:
                if retries == 1:
                    tracking_dict.pop(0)
                    tracking_dict.pop(1)
                    r_from = ps.log.seconds
            elif node_id == 3:
                retries += 1
            if node_id in end_dict:
                for target in end_dict[node_id]:
                    start = tracking_dict.get(target[0], None)
                    if start is not None:
                        end = ps.log.seconds
                        ret_dict[target].append((start, end, end-start))
            if retries < 2 and node_id in end_d_dict:
                for target in end_d_dict[node_id]:
                    start = tracking_dict.get(target[0], None)
                    if start is not None:
                        end = ps.log.seconds
                        ret_d_dict[target].append((start, end, end-start))
            if ps.nxt:
                tracking_dict[node_id] = ps.nxt.log.seconds
            ps = ps.nxt

        retry_interval = None
        if r_from is not None:
            retry_interval = (r_from, self.pace.log.seconds,
                              self.pace.log.seconds-r_from)

        return ret_dict, ret_d_dict, retry_interval

    def report(self):
        if self.state in [self.SUCCESS,
                          self.NOVALIDHOST,
                          self.MAXATTEMPT]\
                and not self.error_logs and not self.more_logs:
            return
        print("")
        print(">>> NEW State Machine #%s" % self.uuid)
        print("  retries: %d" % self.retries)
        print(" <LOG>")
        ps = self.paces[0]
        ps = ps.nxt
        while ps is not None:
            if ps.log:
                print(ps.log)
            else:
                print("empty: pace %s, node %s" % (ps.id, ps.node.id))
            ps = ps.nxt
        if self.extras:
            print(" <EXTRAS>")
            for lg in self.extras:
                print(lg)
        if self.error_logs:
            print(" <ERRORS>")
            for lg in self.error_logs:
                print(lg)
        if self.more_logs:
            print(" <MORE LOGS>")
            for lg in self.more_logs:
                print(lg)

        print("Evaluation: %s; %s" % (self.state, self.state_msg))


class RequestStateMachinesParser(object):
    def __init__(self, stms):
        self.total_requests = stms
        self.success_requests = []
        self.nvh_requests = []
        self.retry_failed_requests = []
        self.api_failed_requests = []
        self.incomplete_requests = []
        self.error_requests = []

        self._available_stms = []

        self.len_all_queries = 0
        self.len_direct_successful_queries = 0
        self.len_direct_nvh_queries = 0
        self.len_direct_rtf_queries = 0
        self.len_retry_successful_queries = 0
        self.len_retry_nvh_queries = 0
        self.len_retry_retried_queries = 0

        for stm in stms.values():
            if stm.state == RequestStateMachine.SUCCESS:
                self.success_requests.append(stm)
                if stm.retries == 1:
                    self.len_direct_successful_queries += 1
                else:
                    self.len_direct_rtf_queries += 1
                    self.len_retry_retried_queries += (stm.retries-2)
                    self.len_retry_successful_queries += 1
                self.len_all_queries += stm.retries
                self._available_stms.append(stm)
            elif stm.state == RequestStateMachine.NOVALIDHOST:
                self.nvh_requests.append(stm)
                if stm.retries == 1:
                    self.len_direct_nvh_queries += 1
                else:
                    self.len_direct_rtf_queries += 1
                    self.len_retry_retried_queries += (stm.retries-2)
                    self.len_retry_nvh_queries += 1
                self.len_all_queries += stm.retries
                self._available_stms.append(stm)
            elif stm.state == RequestStateMachine.MAXATTEMPT:
                self.retry_failed_requests.append(stm)
                self.len_direct_rtf_queries += 1
                self.len_retry_retried_queries += (stm.retries-1)
                self.len_all_queries += stm.retries
                self._available_stms.append(stm)
            elif stm.state == RequestStateMachine.FAILED:
                if "api failed" in stm.state_msg:
                    self.api_failed_requests.append(stm)
                else:
                    self.error_requests.append(stm)
                self._available_stms.append(stm)
            elif stm.state == RequestStateMachine.INCOMPLETE:
                self.incomplete_requests.append(stm)
            elif stm.state in [RequestStateMachine.ERROR,
                               RequestStateMachine.NOTSTARTED,
                               RequestStateMachine.READY]:
                self.error_requests.append(stm)
            else:
                raise RuntimeError("unrecognized state: %s" % stm.state)

        start_time = sys.maxint
        end_time = 0
        for stm in self._available_stms:
            start_time = min(start_time, stm.paces[1].log.seconds)
            end_time = max(end_time, stm.pace.log.seconds)

        self.wall_clock_total = end_time - start_time

        _i_apif = set([(0, 20)])
        _i_api = set([(0, 20), (0, 2)])
        _i_atc = set([(1, 3)])
        _i_con1 = set([(2, 5), (2, 21)])
        _i_cts = set([(4, 6)])
        _i_sch = set([(5, 11), (5, 12)])
        _i_stc = set([(10, 13), (10, 22)])
        _i_con2 = set([(11, 14)])
        _i_contc = set([(13, 15)])
        _i_com = set([(14, 2), (14, 23), (14, 24)])
        _i_ctcon = set([(16, 3)])
        _i_con = _i_con1 | _i_con2

        _i_fil = set([(6, 10)])
        _i_cac = set([(7, 9)])
        _i_gap = set([(8, 16), (8, 23), (8, 24)])
        _i_sus = set([(0, 24)])

        _i_qall = _i_api | _i_con | _i_sch | _i_com
        _i_qdir = _i_qall | _i_atc | _i_cts | _i_stc | _i_contc \
            | _i_fil | _i_cac | _i_gap | _i_sus | _i_apif

        _dict_complete = collections.defaultdict(list)
        _dict_direct = collections.defaultdict(list)
        _retries = []

        for stm in self._available_stms:
            _c, _d, _r = stm.get_intervals(_i_qall, _i_qdir)

            for k, v in _c.items():
                _dict_complete[k].extend(v)
            for k, v in _d.items():
                _dict_direct[k].extend(v)
            if _r is not None:
                _retries.append(_r)

        def _get_intervals(input_dict, input_set):
            ret = []
            for item in input_set:
                ret.extend(input_dict[item])
            return Intervals(ret)

        self.intervals_api_complete = _get_intervals(_dict_complete, _i_api)
        self.intervals_conductor = _get_intervals(_dict_complete, _i_con)
        self.intervals_sched = _get_intervals(_dict_complete, _i_sch)
        self.intervals_compute = _get_intervals(_dict_complete, _i_com)
        self.intervals_direct_success = _get_intervals(_dict_direct, _i_sus)
        self.wall_clock_query = self.intervals_direct_success.average()

        self.intervals_direct_inapi = _get_intervals(_dict_direct, _i_api)
        self.intervals_direct_a_con = _get_intervals(_dict_direct, _i_atc)
        self.intervals_direct_cond1 = _get_intervals(_dict_direct, _i_con1)
        self.intervals_direct_c_sch = _get_intervals(_dict_direct, _i_cts)
        self.intervals_direct_sched = _get_intervals(_dict_direct, _i_sch)
        self.intervals_direct_s_con = _get_intervals(_dict_direct, _i_stc)
        self.intervals_direct_cond2 = _get_intervals(_dict_direct, _i_con2)
        self.intervals_direct_c_com = _get_intervals(_dict_direct, _i_contc)
        self.intervals_direct_compute = _get_intervals(_dict_direct, _i_com)

        self.intervals_direct_filter = _get_intervals(_dict_direct, _i_fil)
        self.intervals_direct_cache_refresh = _get_intervals(_dict_direct,
                                                             _i_cac)
        self.intervals_direct_gap = _get_intervals(_dict_direct, _i_gap)

        self.intervals_api_fail = _get_intervals(_dict_direct, _i_apif)
        self.intervals_retry = Intervals(_retries)


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
        data0 = [(round(interval[0], 6), 1) for interval in self.intervals]
        data0.extend([(round(interval[1], 6), -1)
                      for interval in self.intervals])
        data0.sort(key=lambda tup: tup[0])

        x_list = []
        y_list = []
        start_x = None
        y = 0
        x = None
        o_y = None
        for data in data0:
            if x is None:
                x = data[0]
                start_x = x
                o_y = y
                y += data[1]
            elif x != data[0]:
                x_list.append(x)
                y_list.append(o_y)
                x_list.append(x+0.00001)
                y_list.append(y)
                x = data[0]
                o_y = y
                y += data[1]
            else:
                y += data[1]
        if x is not None:
            x_list.append(x)
            y_list.append(o_y)
            x_list.append(x+0.00001)
            y_list.append(y)

        return x_list, y_list, start_x

    def x_y_lasts(self):
        x_list = [interval[0] for interval in self.intervals]
        y_list = [interval[2] for interval in self.intervals]
        return x_list, y_list
        
    def print_all(self):
        print("Intervals:")
        for inte in self.intervals:
            print(inte)


def generate_report(args, sm_parser, schedulers, computes):
    # number_of_schedulers
    number_of_active_schedulers = schedulers
    # number_of_computes
    number_of_active_computes = computes
    # number_of_api_workers
    # number_of_conductor_workers
    # number_of_max_retries

    count_total_requests = len(sm_parser.total_requests)
    count_successful_requests = len(sm_parser.success_requests)
    count_nvh_requests = len(sm_parser.nvh_requests)
    count_retry_failed_requests = len(sm_parser.retry_failed_requests)
    count_api_failed_requests = len(sm_parser.api_failed_requests)
    count_incomplete_requests = len(sm_parser.incomplete_requests)
    count_error_requests = len(sm_parser.error_requests)

    count_total_queries = sm_parser.len_all_queries
    count_direct_successful_queries = sm_parser.len_direct_successful_queries
    count_direct_nvh_queries = sm_parser.len_direct_nvh_queries
    count_direct_retried_queries = sm_parser.len_direct_rtf_queries
    count_retried_successful_queries = sm_parser.len_retry_successful_queries
    count_retried_nvh_queries = sm_parser.len_retry_nvh_queries
    count_retried_retried_queries = sm_parser.len_retry_retried_queries

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


class LogFile(object):
    def __init__(self, name, log_file, relation):
        self.service = None
        self.host = None
        self.name = name

        self.log_lines = []

        self.errors = []
        self.logs_by_ins = collections.defaultdict(list)

        self.lo = None
        self.hi = None

        with open(log_file, 'r') as reader:
            for line in reader:
                if "BENCH-" not in line:
                    continue
                if "Bench initiated!" in line:
                    continue
                lg = LogLine(line, self)
                if not lg.get_relation(relation):
                    print("Fail getting relation for line %s in file %s!"
                          % (lg, self.name))
                    return
                self.log_lines.append(lg)

    def set_offset(self, lo, hi):
        if lo is not None:
            if self.lo is None:
                self.lo = lo
            else:
                self.lo = max(self.lo, lo)
        if hi is not None:
            if self.hi is None:
                self.hi = hi
            else:
                self.hi = min(self.hi, hi)
        if self.lo is not None and self.hi is not None \
                and self.lo >= self.hi:
            return False
        else:
            return True

    def correct_seconds(self):
        if self.lo is None:
            return

        if self.hi is None:
            self.hi = self.lo + 0.02

        offset = (self.lo + self.hi) / 2
        for log in self.log_lines:
            log.seconds += offset

    def apply_relation(self, relation, relation_name):
        mismatch_errors = set()
        for i in range(0, len(self.log_lines)):
            ret = self.log_lines[i].apply(relation, relation_name,
                                          self.log_lines, i)
            if ret is not True:
                mismatch_errors.add(ret)
                self.log_lines[i].correct = False
        return mismatch_errors

    def catg_logs(self, name_errors, mismatch_errors):
        for log in self.log_lines:
            if log.correct and log.instance_name not in name_errors \
                    and log.instance_name not in mismatch_errors \
                    and log.instance_id not in mismatch_errors:
                self.logs_by_ins[log.instance_name].append(log)
            else:
                self.errors.append(log)

    def pprint(self):
        print("name: %s" % self.name)
        print("service: %s" % self.service)
        print("host: %s" % self.host)
        print("-----")
        for line in self.log_lines:
            print(repr(line))
        print("<<<<<\n")


class LogCollector(object):
    def __init__(self, log_folder):
        self.api = None
        self.conductor = None
        self.schedulers = {}
        self.computes = {}
        self.log_files = []

        self.relation = {}

        for f in os.listdir(log_folder):
            file_dir = path.join(log_folder, f)
            if not path.isfile(file_dir):
                continue
            if not f.endswith(".log"):
                continue
            if f.startswith("out"):
                continue
            f = LogFile(f, file_dir, self.relation)
            if f.service == "api":
                if not self.api:
                    self.api = f
                    self.log_files.append(f)
                else:
                    raise RuntimeError("There's already a log for api: %s, "
                                       "but there is another one: %s"
                                       % (self.api.name, f.name))
            elif f.service == "conductor":
                if not self.conductor:
                    self.conductor = f
                    self.log_files.append(f)
                else:
                    raise RuntimeError("There's already a log for conductor: "
                                       "%s, but there is another one: %s"
                                       % (self.conductor.name, f.name))
            elif f.service == "scheduler":
                if f.host not in self.schedulers:
                    self.schedulers[f.host] = f
                    self.log_files.append(f)
                else:
                    raise RuntimeError("There's already a log for scheduler: "
                                       "%s, but there is another one: %s" %
                                       (self.schedulers[f.host].name, f.name))
            elif f.service == "compute":
                if f.host not in self.computes:
                    self.computes[f.host] = f
                    self.log_files.append(f)
                else:
                    raise RuntimeError("There's already a log for compute: "
                                       "%s, but there is another one: %s" %
                                       (self.computes[f.host].name, f.name))
            else:
                if f.log_lines:
                    raise RuntimeError("Unrecognized service %s for file %s" %
                                       (f.service, f.name))
        if not self.api or not self.conductor or not self.schedulers \
                or not self.computes:
            raise RuntimeError("Incomplete log files.")

    def process_logs(self):
        name_errors = set()
        mismatch_errors = set()

        # check name duplication
        relation_name = {}
        for rel in self.relation.values():
            rel_record = relation_name.get(rel.instance_name, None)
            if rel_record:
                print("Warn! relation has duplicated name! %s, %s"
                      % (rel_record.get_line(), rel.get_line()))
                name_errors.add(rel.instance_name)
            relation_name[rel.instance_name] = rel

        # apply relations
        for lf in self.log_files:
            mismatch_errors.union(lf.apply_relation(self.relation,
                                                    relation_name))

        for lf in self.log_files:
            lf.catg_logs(name_errors, mismatch_errors)

        return name_errors, mismatch_errors

    def emit_logs(self):
        controller_logs = collections.defaultdict(list)
        compute_logs = self.computes

        active_schedulers, active_computes = 0, 0

        for key, values in self.api.logs_by_ins.items():
            controller_logs[key].extend(values)
        for key, values in self.conductor.logs_by_ins.items():
            controller_logs[key].extend(values)
        for sche in self.schedulers.values():
            if sche.logs_by_ins:
                active_schedulers += 1
                for key, values in sche.logs_by_ins.items():
                    controller_logs[key].extend(values)
        for logs in controller_logs.values():
            logs.sort(key=lambda item: item.seconds)

        for comp in self.computes.values():
            if comp.logs_by_ins:
                active_computes += 1

        return controller_logs, compute_logs,\
            active_schedulers, active_computes


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
    args = parser.parse_args()
    current_path = path.dirname(os.path.realpath(__file__))
    current_path = path.join(current_path, args.folder)

    log_collector = LogCollector(current_path)
    name_errors, mismatch_errors = log_collector.process_logs()
    controller_logs, compute_logs, active_schedulers, active_computes =\
        log_collector.emit_logs()

    graph = Diagram()
    
    group_by_state_machine1 = {}
    status_count1 = collections.defaultdict(int)
    for name, logs in controller_logs.items():
        s1 = RequestStateMachine(name, None, graph)
        s1.parse_controller(logs)
        group_by_state_machine1[name] = s1

    for stm in group_by_state_machine1.values():
        stm.parse_computes(compute_logs)
    
    for comp_log in compute_logs.values():
        comp_log.correct_seconds()

    for stm in group_by_state_machine1.values():
        stm.check()
        stm.report()
        status_count1[stm.state] += 1

    print("")
    print(" >> LOG SUMMARY")
    print("Active schedulers: %d" % active_schedulers)
    print("Active computes: %d" % active_computes)
    print("Total requests count: %d" % sum(status_count1.values()))
    for key in status_count1.keys():
        print ("  %s requests: %d" % (key, status_count1[key]))
    if name_errors:
        print("duplicated instance names: %s" % name_errors)
    if mismatch_errors:
        print("mismatched names and ids: %s" % mismatch_errors)
    if args.brief:
        return

    sm_parser1 = RequestStateMachinesParser(group_by_state_machine1)
    generate_report(args,
                    sm_parser1,
                    active_schedulers,
                    active_computes)

if __name__ == "__main__":
    main()
