import abc
from collections import defaultdict

from state_machine import LeafGraph
from state_machine import MasterGraph


class ParseError(Exception):
    pass


class PaceBase(object):
    """ Pace is relative to transition. """
    def __init__(self, content, transition, from_node, to_node, from_pace):
        self.transition = transition
        self.content = content
        self.from_node = from_node
        self.to_node = to_node

        self.nxt = None
        self.bfo = None
        if from_pace is not None:
            from_pace.connect(self)

    @property
    def state(self):
        return self.to_node

    @property
    def ident(self):
        return self.content.ident

    def connect(self, p):
        assert isinstance(p, self.__class__)
        assert self.nxt is None
        assert p.bfo is None

        self.nxt = p
        p.bfo = self

    @abc.abstractproperty
    def assume_host(self):
        return None

    @abc.abstractmethod
    def confirm_pace(self, content):
        assert isinstance(content, self.content.__class__)
        return None


class LeafPace(PaceBase):
    """ LeafPace is relative to edge/log. """
    def __init__(self, log, from_node, edge, from_pace=None):
        super(LeafPace, self).__init__(
            log, edge, from_node, edge.node, from_pace)

    @property
    def assume_host(self):
        f_assume_host = self.edge.f_assume_host
        if f_assume_host is not None:
            return f_assume_host(self.log.action)
        else:
            return None

    def confirm_pace(self, log):
        super(LeafPace, self).confirm_pace(log)
        edge = self.to_node.decide_edge(log)
        if edge:
            p = LeafPace(log, self.to_node, edge, self)
            return p
        else:
            return None

    @property
    def log(self):
        return self.content

    @property
    def edge(self):
        return self.transition

    def __repr__(self):
        return "<LeafPace %s %s %s>" % (self.log.request_id,
                                        self.log.seconds,
                                        self.log.action)


class InstanceBase(object):
    def __init__(self, graph, ident):
        self.graph = graph
        self.ident = ident

        self.from_pace = None
        self.to_pace = None

        self.fail_message = ""

    @property
    def from_node(self):
        return self.from_pace.from_node

    @property
    def to_node(self):
        return self.to_pace.to_node

    @property
    def is_end(self):
        if self.to_pace and self.to_pace.to_node in self.graph.end_nodes:
            return True
        else:
            return False

    @property
    def is_failed(self):
        return bool(self.fail_message) or not self.is_end

    @property
    def state(self):
        return self.to_pace.state


class LeafInstance(InstanceBase):
    def __init__(self, graph, ident, host):
        assert isinstance(graph, LeafGraph)
        super(LeafInstance, self).__init__(graph, ident)

        self.host = host
        self.extra_logs = {}

    @property
    def from_edge(self):
        return self.from_pace.edge

    @property
    def service(self):
        return self.graph.service

    @property
    def assume_host(self):
        if not self.is_end:
            raise ParseError("LeafIns %s is not end!" % self)
        return self.to_pace.assume_host

    @property
    def sort_key(self):
        return self.from_pace.log.seconds

    def connect(self, ins):
        self.to_pace.connect(ins.from_pace)

    def confirm(self, log):
        if self.ident != log.ident:
            return False

        if not self.is_end:
            p = self.to_pace.confirm_pace(log)
            assert self.host == log.host
            if p is not None:
                self.to_pace = p
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
        p = self.from_pace
        while p:
            ret_str += "\n    %s" % p
            if p is self.to_pace:
                break
            p = p.nxt

        if self.extra_logs:
            ret_str += "\nExtra_logs:"
            for log in self.extra_logs.itervalues():
                ret_str += "\n    %s" % log

        ret_str += "\n"
        return ret_str


class NestedPace(PaceBase):
    """ NestedPace is relative to subgraph/subinstance. """
    def __init__(self, sub_instance, from_pace=None):
        super(NestedPace, self).__init__(
            sub_instance, sub_instance.graph, sub_instance.from_node,
            sub_instance.to_node, from_pace)

    @property
    def sub_instance(self):
        return self.content

    @property
    def assume_host(self):
        return self.content.assume_host

    def connect(self, p):
        super(NestedPace, self).connect(p)
        self.sub_instance.connect(p.sub_instance)

    def confirm_pace(self, ins):
        super(NestedPace, self).confirm_pace(ins)
        host = self.assume_host
        if host is not None and ins.host != host:
            return None
        if self.to_node.accept_edge(ins.from_edge):
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


class NestedInstance(InstanceBase):
    def __init__(self, graph, ident):
        assert isinstance(graph, MasterGraph)
        super(NestedInstance, self).__init__(graph, ident)

    def confirm(self, ins):
        if self.is_end:
            return False

        p = None
        if self.to_pace is None:
            for node in self.graph.start_nodes:
                if node.accept_edge(ins.from_edge):
                    p = NestedPace(ins)
                    self.from_pace = p
                    break
        else:
            p = self.to_pace.confirm_pace(ins)

        if p:
            self.to_pace = p
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

        if self.to_pace is None:
            for node in self.graph.start_nodes:
                for edge in node.edges:
                    graphs.add(edge.graph)
        else:
            node = self.to_pace.to_node
            for edge in node.edges:
                graphs.add(edge.graph)
        return graphs

    @property
    def assume_host(self):
        if self.is_end or self.to_pace is None:
            return None
        else:
            return self.to_pace.assume_host

    def __repr__(self):
        ret_str = "<NestedIns ident:%s graph:%s end:%s state:%s>" \
                  % (self.ident, self.graph.name, self.is_end, self.state)
        return ret_str

    def __str__(self):
        ret_str = "%r:" % self

        ret_str += "\nPaces:"
        p = self.from_pace
        while p:
            ret_str += "\n    %r" % p
            if p is self.to_pace:
                break
            p = p.nxt
        ret_str += "\n"
        p = self.from_pace
        while p:
            ret_str += "\n%s" % p.sub_instance
            if p is self.to_pace:
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
            instances.sort(key=lambda ins: ins.sort_key)

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

        # step 1: build leaf instances
        logs_by_service_host = self.log_collector.service_host_dict
        for service, logs_by_host in logs_by_service_host.iteritems():
            for host, log_file in logs_by_host.iteritems():
                for log in log_file.log_lines:
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
                            raise RuntimeError(
                                "Unrecognized logline: %s" % log)
                        else:
                            ins = LeafInstance(graph, log.ident, log.host)

                            node, edge = ins.graph.decide_node_edge(log)
                            if node is None or edge is None:
                                raise ParseError(
                                    "Unrecognized init_log:%s" % log)
                            p = LeafPace(log, node, edge, None)
                            ins.from_pace = p
                            ins.to_pace = p

                            helper.add(ins)

        for helper in helpers_by_ident.values():
            helper.sort()

        class BreakIt(Exception):
            pass

        # step 2: build nested instances
        instances = []
        for ident, helper in helpers_by_ident.iteritems():
            instance = NestedInstance(self.graph, ident)

            try:
                while True:
                    graphs = instance.assume_graphs()
                    if not graphs:
                        break

                    try:
                        for graph in graphs:
                            host = instance.assume_host
                            for ins in helper.iter_by_graph(graph, host):
                                if instance.confirm(ins):
                                    helper.remove(ins)
                                    raise BreakIt()
                    except BreakIt:
                        pass
                    else:
                        instance.fail_message = "%r cannot find next LeafInstance!" \
                                                % instance
                        break
            except ParseError as e:
                instance.fail_message = e.message

            if instance.fail_message:
                pass
            elif helper:
                instance.fail_message = "%r has unexpected instances!" \
                                        % instance
            elif instance.is_end is False:
                instance.fail_message = "%r is not ended!" % instance

            if instance.is_failed:
                print "PARSE FAIL >>>>>>>>>>>>>>>>>>>"
                print "Fail message"
                print "------------"
                print instance.fail_message
                print ""
                print "Parsed instance"
                print "---------------"
                print instance
                if helper:
                    print("Unexpected instances")
                    print "--------------------"
                    for ins in helper.instance_set:
                        print("\n%s" % ins)

            instances.append(instance)

        return instances
