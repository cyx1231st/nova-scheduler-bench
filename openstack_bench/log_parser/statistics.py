from collections import defaultdict
from numbers import Integral
from numbers import Real

from state_graph import MasterGraph


class Interval(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.duration = end - start

    def __repr__(self):
        ret_str = "<%.3f, %.3f -> %.3f>" \
                  % (self.start, self.end, self.duration)
        return ret_str


class Intervals(object):
    def __init__(self, intervals):
        assert isinstance(intervals, list)
        self.intervals = intervals
        self.intervals.sort(key=lambda interv: (interv.start, interv.end))
        self._w_t = None
        self._ave = None

    def wall_time(self):
        if self._w_t is None:
            total = 0
            start = None
            end = None
            for inte in self.intervals:
                if start is None:
                    start = inte.start
                    end = inte.end
                elif inte.start <= end:
                    end = max(end, inte.end)
                else:
                    total += (end-start)
                    start = inte.start
                    end = inte.end
            if start is not None:
                total += (end-start)
            self._w_t = total
        return self._w_t

    def average(self):
        if self._ave is None:
            if not self.intervals:
                self._ave = 0
            else:
                self._ave = sum([interval.duration
                                 for interval in self.intervals]) /\
                    len(self.intervals)
        return self._ave

    def x_y_incremental(self):
        data0 = [(round(interval.start, 6), 1) for interval in self.intervals]
        data0.extend([(round(interval.end, 6), -1)
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
        x_list = [interval.start for interval in self.intervals]
        y_list = [interval.duration for interval in self.intervals]
        return x_list, y_list

    def __str__(self):
        ret_str = "Intervals:"
        for inte in self.intervals:
            ret_str += "\n   %s" % inte
        return ret_str

    def __len__(self):
        return len(self.intervals)


class Report(object):
    def __init__(self, name):
        self.outfile = None
        self.contents = []
        self.key_len = 0
        self.name = name
        self.register("Name", name)

    def register(self, key, value):
        key = key
        self.contents.append((key, value))
        self.key_len = max(self.key_len, len(key))

    def blank(self):
        self.contents.append(None)

    def set_outfile(self, outfile, print_header):
        self.outfile = outfile
        self.print_header = print_header

    def export(self):
        if self.outfile is None:
            print("\n >> FINAL REPORT:")
            for content in self.contents:
                if content is None:
                    print("")
                elif isinstance(content[1], Integral):
                    format_str = "{:<" + str(self.key_len + 2) + "}{:d}"
                    print(format_str.format(content[0]+":", content[1]))
                elif isinstance(content[1], Real):
                    format_str = "{:<" + str(self.key_len + 2) + "}{:7.5f}"
                    print(format_str.format(content[0]+":", content[1]))
                else:
                    format_str = "{:<" + str(self.key_len + 2) + "}{:s}"
                    print(format_str.format(content[0]+":", content[1]))
        else:
            outfile = open(self.outfile, "a+")
            if self.print_header:
                header_fields = [content[0] for content in self.contents
                                 if content is not None]
                outfile.write(",".join(header_fields) + '\n')
            row_fields = [str(content[1]) for content in self.contents
                          if content is not None]
            outfile.write(",".join(row_fields) + '\n')
            outfile.flush()
            outfile.close()


class Engine(object):
    def __init__(self, graph, instances, log_collector):
        assert isinstance(instances, dict)
        assert isinstance(graph, MasterGraph)
        self.instances = instances
        self.graph = graph
        self.available_instances = []
        self.error_instances = []

        constraints = self.check()
        self.relax_constraints(constraints, log_collector)
        self.parse()

    def check(self):
        requests = 0
        adjust_requests = 0
        adjust_points = set()
        constraints = Constraints()

        for instance in self.instances.itervalues():
            requests += 1
            if instance.is_failed or instance.state in ["UNKNOWN", "-"]:
                self.error_instances.append(instance)
                continue
            else:
                self.available_instances.append(instance)

            last_ins = None
            adjust = False
            for leaf_ins in instance:
                if last_ins:
                    constraints.add(last_ins.host, last_ins.to_seconds,
                                    leaf_ins.host, leaf_ins.from_seconds)
                    if last_ins.to_seconds > leaf_ins.from_seconds:
                        adjust_points.add((last_ins.host, leaf_ins.host))
                        if not adjust:
                            adjust = True
                            adjust_requests += 1
                last_ins = leaf_ins

        self.total_requests = requests
        self.requests_to_adjust = adjust_requests
        self.points_to_adjust = adjust_points
        return constraints

    def relax_constraints(self, constraints, log_collector):
        print "-"*20
        print("\n >> CONSTRAINT REPORT:")

        print "Violated requests: %s" % self.requests_to_adjust
        print "Violated constraints:"
        conp_list, host_dict = constraints.group_by_host()
        min_dist = None
        for conp in conp_list:
            if conp.violated:
                print "    %s" % conp
            dist = conp.distance
            if dist is not None:
                if min_dist is None:
                    min_dist = dist
                else:
                    min_dist = min(min_dist, dist)
        print "Min distance: %.3f" % min_dist

        c_engine = CausalEngine(host_dict, conp_list, min_dist/2)
        c_engine.relax(conp_list[0].from_host)

        print "Adjustion result(%d):" % c_engine.counter
        hosts = [host for host in c_engine.hosts.values()]
        hosts.sort(key=lambda host: host.name)
        for host in hosts:
            if host.low != 0:
                print "    %s" % host

        print
        print "-"*20

        for log_file in log_collector.log_files:
            name = log_file.host
            log_file.correct(c_engine.hosts[name].high)

        # check again
        """
        print
        constraints = self.check()
        print "Violated requests: %s" % self.requests_to_adjust
        print "Violated constraints:"
        conp_list, host_dict = constraints.group_by_host()
        min_dist = None
        for conp in conp_list:
            if conp.violated:
                print "    %s" % conp
            dist = conp.distance
            if dist is not None:
                if min_dist is None:
                    min_dist = dist
                else:
                    min_dist = min(min_dist, dist)
        print
        print "-"*20
        """

    def parse(self):
        # sets
        hosts_set_by_service = defaultdict(set)

        # counters
        requests_by_state = defaultdict(lambda: 0)
        count_by_node = defaultdict(lambda: 0)

        # intervals
        intervals_of_requests = []
        intervals_by_services = defaultdict(list)
        intervals_by_names = defaultdict(list)
        intervals_by_comms = defaultdict(list)

        for instance in self.available_instances:
            requests_by_state[instance.state] += 1

            interval = Interval(instance.from_seconds,
                                instance.to_seconds)
            intervals_of_requests.append(interval)

            last_ins = None
            for leaf_ins in instance:
                if last_ins:
                    comm_key = (last_ins.to_pace.from_node.id_,
                                leaf_ins.from_pace.to_node.id_)
                    interval = Interval(last_ins.to_seconds,
                                        leaf_ins.from_seconds)
                    intervals_by_comms[comm_key].append(interval)

                interval = Interval(leaf_ins.from_seconds,
                                    leaf_ins.to_seconds)
                intervals_by_services[leaf_ins.service].append(interval)
                intervals_by_names[leaf_ins.name].append(interval)

                hosts_set_by_service[leaf_ins.service].add(leaf_ins.host)

                last_ins = leaf_ins

            count_by_node[instance.from_node.id_] += 1
            for p in instance.iterall():
                count_by_node[p.to_node.id_] += 1

        self.active_by_service = defaultdict(lambda: 0)
        self.requests_by_state = requests_by_state
        self.count_by_node = count_by_node

        self.intervals_requests = Intervals(intervals_of_requests)
        self.intervals_requests.wall_time()

        self.intervals_by_services = {}
        self.intervals_by_names = {}
        self.intervals_by_comms = {}

        for service, host_set in hosts_set_by_service.iteritems():
            self.active_by_service[service] = len(host_set)
        for service, intervals in intervals_by_services.iteritems():
            self.intervals_by_services[service] = Intervals(intervals)
        for name, intervals in intervals_by_names.iteritems():
            self.intervals_by_names[name] = Intervals(intervals)
        for comm, intervals in intervals_by_comms.iteritems():
            self.intervals_by_comms[comm] = Intervals(intervals)

    def count(self, node_id):
        return self.count_by_node.get(node_id, 0)

    def report(self, name):
        report = Report(name)

        service_keys = sorted(list(self.graph.services))
        name_keys = sorted(list(self.graph.names))
        state_keys = sorted(list(self.graph.states))
        comm_keys = sorted(self.intervals_by_comms.keys())

        for service in service_keys:
            report.register("active " + service,
                            self.active_by_service.get(service, 0))
        report.blank()
        report.register("total requests",
                        self.total_requests)
        for state in state_keys:
            report.register(state + " requests",
                            self.requests_by_state.get(state, 0))
        report.register("unadjusted requests",
                        self.requests_to_adjust)
        report.register("unadjusted points",
                        self.points_to_adjust)
        report.blank()
        report.register("wallclock total",
                        self.intervals_requests.wall_time())
        for service in service_keys:
            report.register("wallclock " + service,
                            self.intervals_by_services[service].wall_time())
        report.blank()
        for name in name_keys:
            report.register("time " + name + " avg",
                            self.intervals_by_names[name].average())
        for comm in comm_keys:
            report.register("time " + str(comm) + " avg",
                            self.intervals_by_comms[comm].average())

        report.export()

    def extract_intervals(self, targets, cut_targets, cut_edge):
        end_dict = defaultdict(list)
        m_targets = set()
        for target in targets:
            m_targets |= target
        for target in m_targets:
            if target[0] not in self.graph.tracked_nodes_by_id:
                raise RuntimeError("Node#%s cannot find in graph"
                                   % target[0])
            if target[1] not in self.graph.tracked_nodes_by_id:
                raise RuntimeError("Node#%s cannot find in graph"
                                   % target[1])
            end_dict[target[1]].append(target)

        end_c_dict = defaultdict(list)
        m_cut_targets = set()
        for target in cut_targets:
            m_cut_targets |= target
        for target in m_cut_targets:
            if target[0] not in self.graph.tracked_nodes_by_id:
                raise RuntimeError("Node#%s cannot find in graph"
                                   % target[0])
            if target[1] not in self.graph.tracked_nodes_by_id:
                raise RuntimeError("Node#%s cannot find in graph"
                                   % target[1])
            end_c_dict[target[1]].append(target)

        ret_dict = defaultdict(list)
        ret_c_dict = defaultdict(list)
        ret_cutted = []
        for instance in self.available_instances:
            tracking = Tracking()
            is_cut = False
            cut_time = None
            for p in instance.iterall():
                edge = p.edge
                from_node_id = p.from_node.id_
                node_id = p.to_node.id_
                tracking.step(from_node_id, p.log.seconds, node_id)
                if node_id in end_dict:
                    for target in end_dict[node_id]:
                        start = tracking[target[0]]
                        if start is not None:
                            end = p.log.seconds
                            ret_dict[target].append(Interval(start, end))
                if is_cut is False and node_id in end_c_dict:
                    for target in end_c_dict[node_id]:
                        start = tracking[target[0]]
                        if start is not None:
                            end = p.log.seconds
                            ret_c_dict[target].append(Interval(start, end))
                if edge is cut_edge and is_cut is False:
                    is_cut = True
                    cut_time = p.log.seconds

            if cut_time is not None:
                ret_cutted.append(Interval(cut_time, instance.to_seconds))

        intervals_dict = {}
        for target in targets:
            content = []
            for t in target:
                content.extend(ret_dict[t])
            intervals_dict[target] = Intervals(content)
        c_intervals_dict = {}
        for target in cut_targets:
            content = []
            for t in target:
                content.extend(ret_c_dict[t])
            c_intervals_dict[target] = Intervals(content)
        cutted_intervals = Intervals(ret_cutted)

        return intervals_dict, c_intervals_dict, cutted_intervals


class Tracking(object):
    def __init__(self):
        self.node_history = {}
        self.node_tracked = {}
        self.mask_tracked = {}
        self.key = None

    def step(self, from_key, value, to_key):
        assert value is not None
        if self.key is not None:
            assert self.key == from_key
        self.key = to_key
        self.node_history[from_key] = self.node_tracked.copy()
        self.node_tracked[from_key] = value
        self.mask_tracked = self.node_history.get(to_key, {})

    def __getitem__(self, key):
        tracked = self.node_tracked.get(key)
        masked = self.mask_tracked.get(key)
        if tracked == masked:
            return None
        else:
            return tracked


class Constraints(object):
    def __init__(self):
        self.constraints_by_from = defaultdict(dict)
        self.size = 0

    def add(self, from_host, from_, to_host, to):
        if from_host == to_host:
            return
        if to_host not in self.constraints_by_from[from_host]:
            constraint = Constraint(from_host, to_host)
            self.constraints_by_from[from_host][to_host] = constraint
            self.size += 1
        else:
            constraint = self.constraints_by_from[from_host][to_host]
        constraint.adjust(from_, to)

    def group_by_host(self):
        host_dict = defaultdict(dict)
        host_dict1 = defaultdict(list)
        for from_host, to_dict in self.constraints_by_from.iteritems():
            for to_host, constraint in to_dict.iteritems():
                conp = host_dict[from_host].get(to_host)
                if not conp:
                    conp = ConstraintPair(constraint)
                    host_dict[from_host][to_host] = conp
                    host_dict[to_host][from_host] = conp
                    host_dict1[from_host].append(conp)
                    host_dict1[to_host].append(conp)
                else:
                    conp.set(constraint)

        host_keys = host_dict1.keys()
        host_keys.sort(key=lambda host: len(host_dict1[host]), reverse=True)
        ret_list = []
        for key in host_keys:
            c_list = host_dict1[key]
            for c in c_list:
                if not c.adjusted and c.from_host != key:
                    c.reverse()
                    assert c.from_host == key
            c_list.sort(key=lambda key: key.to_host)
            for c in c_list:
                if not c.adjusted:
                    c.adjusted = True
                    ret_list.append(c)
        return ret_list, host_dict1

    def __str__(self):
        ret_str = "Constraints(%d):" % self.size
        for to_dict in self.constraints_by_from.itervalues():
            for cons in to_dict.itervalues():
                if cons.violated:
                    ret_str += "\n   %s" % cons
        return ret_str


class ConstraintPair(object):
    def __init__(self, constraint):
        self.from_host = constraint.from_host
        self.to_host = constraint.to_host
        self.offset = constraint.offset
        self.r_offset = None
        self.adjusted = False

    def set(self, constraint):
        assert self.r_offset is None
        assert self.from_host == constraint.to_host
        assert self.to_host == constraint.from_host
        self.r_offset = constraint.offset

    @property
    def violated(self):
        if self.offset is not None and self.offset <= 0:
            return True
        if self.r_offset is not None and self.r_offset <= 0:
            return True
        return False

    @property
    def distance(self):
        if self.offset is None or self.r_offset is None:
            return None
        else:
            return self.offset + self.r_offset

    def reverse(self):
        self.from_host, self.to_host = self.to_host, self.from_host
        self.offset, self.r_offset = self.r_offset, self.offset

    def __repr__(self):
        if self.r_offset is None:
            r_offset = "None"
        else:
            r_offset = "%.3f" % -self.r_offset
        if self.offset is None:
            offset = "None"
        else:
            offset = "%.3f" % self.offset
        return "<ConsP %s -> %s: (%s, %s)>" \
               % (self.from_host, self.to_host,
                  r_offset, offset)


class Constraint(object):
    def __init__(self, from_host, to_host):
        self.from_host = from_host
        self.to_host = to_host
        self.offset = None
        self.adjusted = False

    def adjust(self, from_, to):
        offset = to - from_
        if self.offset is None:
            self.offset = offset
        else:
            self.offset = min(self.offset, offset)

    @property
    def violated(self):
        return self.offset < 0 or self.offset == 0

    def __repr__(self):
        return "<Cons %s -> %s: %7.5f>" \
               % (self.from_host, self.to_host, self.offset)


class HostConstraint(object):
    def __init__(self, name):
        self.name = name
        self.low = float("-inf")
        self.high = float("inf")

        # history
        self.ll = None
        self.hh = None

        self.constraints = []

    def change(self, low, high):
        assert self.low <= low\
            and low <= high\
            and high <= self.high

        if self.low != low or self.high != high:
            self.ll = self.low
            self.hh = self.high
            self.low = low
            self.high = high
            return True
        else:
            return False

    def relax(self):
        count = 0
        changed_hosts = set()
        for cons in self.constraints:
            hosts = cons.relax()
            count += 1
            for host in hosts:
                changed_hosts.add(host)
        return changed_hosts, count

    @property
    def determined(self):
        return self.low == self.high

    @property
    def ready(self):
        if self.low != float("-inf") and self.high != float("inf") \
                and self.low != self.high:
            return True
        else:
            return False

    def adjust(self, distance):
        if self.ready:
            # This may introduce offsets even in single-host mode
            if self.low + 2 * distance > self.high:
                result = (self.low + self.high)/2
                self.change(result, result)
                return True
            guess = self.low + distance
            if guess > 0:
                assert guess + distance <= self.high
                self.change(guess, guess)
                return True
            guess = self.high - distance
            if guess < 0:
                assert guess - distance >= self.low
                self.low = guess
                self.high = guess
                return True
            self.change(0, 0)
            return True
        else:
            return False

    def adjust_side(self, distance):
        if self.low == float("-inf") and self.high == float("inf"):
            return False
        elif self.low == float("-inf"):
            if self.high - distance >= 0:
                target = 0
            else:
                target = self.high - distance
            self.change(target, target)
            return True
        elif self.high == float("inf"):
            if self.low + distance <= 0:
                target = 0
            else:
                target = self.low + distance
            self.change(target, target)
            return True
        else:
            return False

    def adjust_none(self):
        assert self.low == float("-inf")
        assert self.high == float("inf")
        self.change(0, 0)

    def __repr__(self):
        if self.determined:
            return "<Host %s is %.5f, %d>" \
                   % (self.name, self.low, len(self.constraints))
        else:
            return "<Host %s, (%.5f, %.5f), %d>" \
                   % (self.name, self.low, self.high, len(self.constraints))


class DirectedConstraint(object):
    def __init__(self, from_host, to_host, r_offset, offset):
        self.from_ = from_host
        self.to_ = to_host
        if r_offset is None:
            self.low = float("-inf")
        else:
            self.low = -r_offset
        if offset is None:
            self.high = float("inf")
        else:
            self.high = offset

    def relax(self):
        changed = []
        low = max(self.low, self.to_.low - self.from_.high)
        high = min(self.high, self.to_.high - self.from_.low)
        if low > high:
            raise RuntimeError("Constraint offset %s violated!" % self)
        self.low = low
        self.high = high

        from_l = max(self.from_.low, self.to_.low - self.high)
        from_h = min(self.from_.high, self.to_.high - self.low)
        if from_l > from_h:
            raise RuntimeError("Constraint from %s violated!" % self)
        try:
            if self.from_.change(from_l, from_h):
                changed.append(self.from_)
        except AssertionError:
            raise RuntimeError("Constraint from_ %s violated!" % self)

        to_l = max(self.to_.low, self.from_.low + self.low)
        to_h = min(self.to_.high, self.from_.high + self.high)
        if to_l > to_h:
            raise RuntimeError("Constraint to %s violated!" % self)
        try:
            if self.to_.change(to_l, to_h):
                changed.append(self.to_)
        except AssertionError:
            raise RuntimeError("Constraint to_ %s violated!" % self)

        return changed

    def __repr__(self):
        return "<DCon %s->%s (%s, %s)>" \
               % (self.from_, self.to_, self.low, self.high)


class CausalEngine(object):
    def __init__(self, host_dict, conp_list, distance):
        self.distance = distance
        self.hosts = {}
        self.unknown_hosts = set()
        self.determined_hosts = set()
        for host in host_dict.iterkeys():
            c_host = HostConstraint(host)
            self.hosts[host] = c_host
            self.unknown_hosts.add(c_host)

        for conp in conp_list:
            from_host = self.hosts[conp.from_host]
            to_host = self.hosts[conp.to_host]
            dcon = DirectedConstraint(from_host, to_host,
                                      conp.r_offset, conp.offset)
            from_host.constraints.append(dcon)
            to_host.constraints.append(dcon)
        self.counter = 0

    def relax(self, i_host):
        init_host = self.hosts[i_host]
        init_host.adjust_none()

        a_host = init_host
        while True:
            self.determined_hosts.add(a_host)
            self.unknown_hosts.remove(a_host)
            if not self.unknown_hosts:
                break

            self.relax_all(a_host)

            a_host = None
            for host in self.unknown_hosts:
                if host.adjust(self.distance):
                    a_host = host
                    break
            if a_host is None:
                for host in self.unknown_hosts:
                    if host.adjust_side(self.distance):
                        a_host = host
                        break
            if a_host is None:
                a_host = self.unknown_hosts[0]
                a_host.adjust_none()

    def relax_all(self, host):
        changed_hosts = {host}
        while changed_hosts:
            next_hosts = set()
            for host in changed_hosts:
                hosts, count = host.relax()
                self.counter += count
                next_hosts.update(hosts)
            changed_hosts = next_hosts
