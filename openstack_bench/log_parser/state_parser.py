from collections import defaultdict

from state_machine import LeafGraph
from state_machine import MasterGraph


class ParseError(Exception):
    pass


class Pace(object):
    """ Pace is relative to transition. """
    def __init__(self, log, from_node, edge, from_pace=None):
        self.transition = edge
        self.content = log
        # state

        self.from_node = from_node
        # to_node

        self.nxt = None
        self.bfo = None

        # edge
        # log

        if from_pace:
            from_pace.connect(self)

    @property
    def to_node(self):
        return self.edge.node

    @property
    def ident(self):
        self.content.ident

    @property
    def state(self):
        return self.to_node

    def connect(self, p):
        assert isinstance(p, Pace)
        assert self.nxt is None
        assert p.bfo is None

        self.nxt = p
        p.bfo = self

    def confirm_pace(self, log):
        edge = self.to_node.decide_edge(log)
        if edge:
            p = Pace(log, self.to_node, edge, self)
            return p
        else:
            return None

    # LeafPace
    @property
    def log(self):
        return self.content

    @property
    def edge(self):
        return self.transition

    @property
    def assume_host(self):
        f_assume_host = self.edge.f_assume_host
        if f_assume_host is not None:
            return f_assume_host(self.log.action)
        else:
            return None

    def __str__(self):
        return "<Pace %s %s %s>" % (self.log.request_id,
                                    self.log.seconds,
                                    self.log.action)


class LeafInstance(object):
    def __init__(self, graph, init_log):
        assert isinstance(graph, LeafGraph)
        self.graph = graph
        self.ident = init_log.ident
        self.host = init_log.host

        node, edge = self.graph.decide_node_edge(init_log)
        if node is None or edge is None:
            raise ParseError("Unrecognized init_log: %s" % init_log)
        p = Pace(init_log, node, edge, None)
        self.start_pace = p
        self.last_pace = p

        self.extra_logs = {}

    @property
    def start_node(self):
        return self.start_pace.from_node

    @property
    def end_node(self):
        return self.last_pace.to_node

    @property
    def start_edge(self):
        return self.start_pace.edge

    @property
    def is_end(self):
        return self.last_pace.to_node in self.graph.end_nodes

    @property
    def service(self):
        return self.graph.service

    @property
    def assume_host(self):
        if not self.is_end:
            raise ParseError("LeafIns %s is not end!" % self)
        return self.last_pace.assume_host

    @property
    def order(self):
        return self.start_pace.log.seconds

    @property
    def state(self):
        return self.last_pace.state

    def confirm(self, log):
        if self.ident != log.ident:
            return False

        if not self.is_end:
            p = self.last_pace.confirm_pace(log)
            assert self.host == log.host
            if p is not None:
                self.last_pace = p
                return True

        edge = self.graph.decide_edge_ignored(log)
        if edge is not None:
            if edge not in self.extra_logs:
                assert self.host == log.host
                self.extra_logs[edge] = log
                return True
            else:
                return False
        else:
            return False

    def __repr__(self):
        ret_str = "<LeafIns ident:%s graph:%s host:%s end:%s state:%s>" \
                  % (self.ident, self.graph.name,
                     self.host, self.is_end, self.state)
        return ret_str

    def __str__(self):
        ret_str = "%r:" % self

        ret_str += "\nPaces:"
        p = self.start_pace
        while p:
            ret_str += "\n    %s" % p
            if p is self.last_pace:
                break
            p = p.nxt

        if self.extra_logs:
            ret_str += "\nExtra_logs:"
            for log in self.extra_logs.itervalues():
                ret_str += "\n    %s" % log

        ret_str += "\n"
        return ret_str

    def connect(self, ins):
        self.last_pace.connect(ins.start_pace)


class NestedPace(object):
    def __init__(self, sub_instance, from_pace=None):
        # transistion
        self.content = sub_instance
        # state

        # from_node
        # to_node

        self.bfo = None
        self.nxt = None

        # sub_instance
        if from_pace:
            from_pace.connect(self)

    @property
    def sub_instance(self):
        return self.content

    @property
    def transistion(self):
        return self.sub_instance.graph

    @property
    def from_node(self):
        return self.sub_instance.start_node

    @property
    def to_node(self):
        return self.sub_instance.end_node

    @property
    def state(self):
        return self.to_node

    @property
    def assume_host(self):
        return self.content.assume_host

    def connect(self, p):
        assert isinstance(p, NestedPace)
        assert self.nxt is None
        assert p.bfo is None

        self.nxt = p
        p.bfo = self
        self.sub_instance.connect(p.sub_instance)

    def confirm_pace(self, ins):
        host = self.assume_host
        if host is not None and ins.host != host:
            return None
        if self.to_node.accept_edge(ins.start_edge):
            p = NestedPace(ins, self)
            return p
        else:
            return None

    # NestedPace
    def __repr__(self):
        ret_str = "<NestPace ins:%r>" % self.sub_instance
        return ret_str

    def __str__(self):
        ret_str = "%r:" % self
        ret_str += "\n%s" % self.sub_instance
        return ret_str


class NestedInstance(object):
    def __init__(self, graph, ident, helper):
        assert isinstance(graph, MasterGraph)
        self.graph = graph
        self.ident = ident

        self.start_pace = None
        self.last_pace = None

        self.fail_message = ""

        class BreakIt(Exception):
            pass

        try:
            while True:
                graphs = self.assume_graphs()
                if not graphs:
                    break

                try:
                    for graph in graphs:
                        host = self.assume_host
                        for ins in helper.iter_by_graph(graph, host):
                            if self.confirm(ins):
                                helper.remove(ins)
                                raise BreakIt()
                except BreakIt:
                    pass
                else:
                    self.fail_message = "%r cannot find next LeafInstance!" \
                                        % self
                    break
        except ParseError as e:
            self.fail_message = e.message

        if self.fail_message:
            pass
        elif helper:
            self.fail_message = "%r has unexpected instances!" % self
        elif self.is_end is False:
            self.fail_message = "%r is not ended!" % self

        if self.fail_message:
            print "PARSE FAIL >>>>>>>>>>>>>>>>>>>"
            print "Fail message"
            print "------------"
            print self.fail_message
            print ""
            print "Parsed instance"
            print "---------------"
            print self
            if helper:
                print("Unexpected instances")
                print "--------------------"
                for ins in helper.instance_set:
                    print("\n%s" % ins)

    @property
    def failed(self):
        return bool(self.fail_message) or self.is_end is False

    @property
    def is_end(self):
        if self.last_pace is not None:
            if self.last_pace.to_node in self.graph.end_nodes:
                return True
        return False

    def confirm(self, ins):
        if self.is_end:
            return False

        p = None
        if self.last_pace is None:
            for node in self.graph.start_nodes:
                if node.accept_edge(ins.start_edge):
                    p = NestedPace(ins)
                    self.start_pace = p
                    break
        else:
            p = self.last_pace.confirm_pace(ins)

        if p:
            self.last_pace = p
            if not ins.is_end:
                raise ParseError("Instance %r is not complete!" % ins)
            else:
                return True
        else:
            return False

    def assume_graphs(self):
        """ Acceptable graphs """
        graphs = set()
        if self.is_end:
            return graphs

        if self.last_pace is None:
            for node in self.graph.start_nodes:
                for edge in node.edges:
                    graphs.add(edge.graph)
        else:
            node = self.last_pace.to_node
            for edge in node.edges:
                graphs.add(edge.graph)
        return graphs

    @property
    def assume_host(self):
        if self.is_end or self.last_pace is None:
            return None
        return self.last_pace.assume_host

    @property
    def state(self):
        return self.last_pace.state

    def __repr__(self):
        ret_str = "<NestedIns ident:%s graph:%s end:%s state:%s>" \
                  % (self.ident, self.graph.name, self.is_end, self.state)
        return ret_str

    def __str__(self):
        ret_str = "%r:" % self

        ret_str += "\nPaces:"
        p = self.start_pace
        while p:
            ret_str += "\n    %r" % p
            if p is self.last_pace:
                break
            p = p.nxt
        ret_str += "\n"
        p = self.start_pace
        while p:
            ret_str += "\n%s" % p.sub_instance
            if p is self.last_pace:
                break
            p = p.nxt
        return ret_str


class EngineHelper(object):
    def __init__(self, ident):
        self.ident = ident
        self.instances_by_service_host = \
            defaultdict(lambda: defaultdict(list))
        self.instances_by_graph_host = \
            defaultdict(lambda: defaultdict(list))
        self.instances_by_graph = defaultdict(list)
        self.instance_set = set()

    def iter_by_sh(self, service, host):
        for ins in self.instances_by_service_host[service][host]:
            if ins._available:
                yield ins

    def iter_by_graph(self, graph, host):
        if host is None:
            for ins in self.instances_by_graph[graph]:
                if ins._available:
                    yield ins
        else:
            for ins in self.instances_by_graph_host[graph][host]:
                if ins._available:
                    yield ins

    def add(self, ins):
        assert ins.ident == self.ident
        assert isinstance(ins, LeafInstance)
        ins._available = True
        self.instances_by_service_host[ins.service][ins.host].append(ins)
        self.instances_by_graph_host[ins.graph][ins.host].append(ins)
        self.instances_by_graph[ins.graph].append(ins)
        self.instance_set.add(ins)

    def sort(self):
        for instances in self.instances_by_graph.itervalues():
            instances.sort(key=lambda ins: ins.order)

    def remove(self, ins):
        assert ins in self.instance_set
        ins._available = False
        self.instance_set.remove(ins)

    def __bool__(self):
        return bool(self.instance_set)
    __nonzero__ = __bool__


class ParserEngine(object):
    def __init__(self, graph, log_collector):
        self.graph = graph
        self.log_collector = log_collector

    def parse(self):
        helpers_by_ident = {}

        def build_service_logs(service, host, logs):
            for log in logs:
                ident = log.ident
                assert ident is not None
                assert service == log.service
                assert host == log.host

                helper = helpers_by_ident.get(ident)
                if not helper:
                    helper = EngineHelper(ident)
                    helpers_by_ident[ident] = helper

                for ins in helper.iter_by_sh(service, host):
                    if ins.confirm(log):
                        break
                else:
                    graph = self.graph.decide_subgraph(log)
                    if not graph:
                        raise RuntimeError("Unrecognized logline: %s" % log)
                    else:
                        ins = LeafInstance(graph, log)
                        helper.add(ins)

        # step 1: build leaf instances
        logs_by_service_host = self.log_collector.service_host_dict
        for service, logs_by_host in logs_by_service_host.iteritems():
            for host, log_file in logs_by_host.iteritems():
                build_service_logs(service, host, log_file.log_lines)

        for helper in helpers_by_ident.values():
            helper.sort()

        # step 2: build nested instances
        instances = []
        for ident, helper in helpers_by_ident.iteritems():
            instances.append(NestedInstance(self.graph,
                                            ident,
                                            helper))
        return instances
