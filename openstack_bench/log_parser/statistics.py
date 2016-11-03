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
        ret_str = "<%.3f, %.3f -> %.3f>" % (self.start, self.end, self.duration)
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
                                 for interval in self.intervals])\
                            / len(self.intervals)
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
    def __init__(self, graph, instances):
        assert isinstance(instances, dict)
        assert isinstance(graph, MasterGraph)
        self.instances = instances
        self.graph = graph
        self.available_instances = []
        self.error_instances = []
        # TODO: carsal constraints

        self.parse()

    def parse(self):
        # sets
        hosts_set_by_service = defaultdict(set)

        # counters
        requests = 0
        requests_by_state = defaultdict(lambda: 0)
        adjust_requests = 0
        adjust_points = set()
        count_by_node = defaultdict(lambda: 0)

        # intervals
        intervals_of_requests = []
        intervals_by_services = defaultdict(list)
        intervals_by_names = defaultdict(list)
        intervals_by_comms = defaultdict(list)

        for instance in self.instances.itervalues():
            requests += 1
            if instance.is_failed or instance.state in ["UNKNOWN", "-"]:
                requests_by_state["PARSE ERROR"] += 1
                self.error_instances.append(instance)
                continue
            else:
                requests_by_state[instance.state] += 1
                self.available_instances.append(instance)

            interval = Interval(instance.from_seconds,
                                instance.to_seconds)
            intervals_of_requests.append(interval)

            last_ins = None
            adjust = False
            for leaf_ins in instance:
                if last_ins:
                    if last_ins.to_seconds > leaf_ins.from_seconds:
                        adjust_points.add((last_ins.host, leaf_ins.host))
                        if not adjust:
                            adjust = True
                            adjust_requests += 1
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
        self.total_requests = requests
        self.requests_by_state = requests_by_state
        self.requests_to_adjust = adjust_requests
        self.points_to_adjust = len(adjust_points)
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
