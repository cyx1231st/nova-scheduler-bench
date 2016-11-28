import abc

from state_graph import LeafGraph
from state_graph import MasterGraph
from state_graph import Node


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

    @abc.abstractproperty
    def from_seconds(self):
        return None

    @abc.abstractproperty
    def to_seconds(self):
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

    @property
    def log(self):
        return self.content

    @property
    def from_seconds(self):
        return self.log.seconds

    @property
    def to_seconds(self):
        return self.log.seconds

    @property
    def edge(self):
        return self.transition

    def confirm_pace(self, log):
        super(LeafPace, self).confirm_pace(log)
        edge = self.to_node.decide_edge(log)
        if edge:
            p = LeafPace(log, self.to_node, edge, self)
            return p
        else:
            return None

    def __repr__(self):
        return "<LeafPace (rid)%s (sec)%s %s>" % (self.log.request_id,
                                                  self.log.seconds,
                                                  self.log.action)


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

    @property
    def from_seconds(self):
        return self.sub_instance.from_seconds

    @property
    def to_seconds(self):
        return self.sub_instance.to_seconds

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
        if self.to_pace is None:
            return None
        else:
            return self.to_pace.to_node

    @property
    def start_leaf_pace(self):
        return None

    @property
    def from_seconds(self):
        return self.from_pace.from_seconds

    @property
    def to_seconds(self):
        return self.to_pace.to_seconds

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
        if not self.to_node:
            return "UNKNOWN"
        else:
            state = self.to_node.state
            if state is Node.UNKNOWN_STATE:
                return "-"
            else:
                return state

    @property
    def assume_host(self):
        if not self.is_end or self.to_pace is None:
            return None
        else:
            return self.to_pace.assume_host

    @property
    def name(self):
        return self.graph.name

    def iterall(self):
        p = self.start_leaf_pace
        while p:
            yield p
            if p.to_node in self.graph.end_nodes:
                break
            p = p.nxt

    def __iter__(self):
        p = self.from_pace
        while p:
            yield p.content
            if p is self.to_pace:
                break
            p = p.nxt

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
        return ret_str


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
    def sort_key(self):
        return self.from_pace.log.seconds

    @property
    def service(self):
        return self.graph.service

    @property
    def start_leaf_pace(self):
        return self.from_pace

    def connect(self, ins):
        self.to_pace.connect(ins.from_pace)

    def confirm(self, log):
        assert self.host == log.host

        if self.ident != log.ident:
            return False

        if not self.is_end:
            p = None
            if self.to_pace is None:
                node, edge = self.graph.decide_node_edge(log)
                if node and edge:
                    p = LeafPace(log, node, edge, None)
                    self.from_pace = p
            else:
                p = self.to_pace.confirm_pace(log)

            if p:
                self.to_pace = p
                return True

        # extra edges handling
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
        ret_str = "<LeafIns ident:%s graph:%s end:%s state:%s host:%s>" \
                  % (self.ident, self.graph.name,
                     self.is_end, self.state, self.host)
        return ret_str

    def __str__(self):
        ret_str = super(LeafInstance, self).__str__()

        if self.extra_logs:
            ret_str += "\nExtra_logs:"
            for log in self.extra_logs.itervalues():
                ret_str += "\n    %s" % log

        ret_str += "\n"
        return ret_str


class NestedInstance(InstanceBase):
    def __init__(self, graph, ident):
        assert isinstance(graph, MasterGraph)
        super(NestedInstance, self).__init__(graph, ident)

    @property
    def start_leaf_pace(self):
        return self.from_pace.content.from_pace

    def confirm(self, ins):
        if self.ident != ins.ident:
            return False

        if not self.is_end:
            p = None
            if self.to_pace is None:
                # TODO self.graph.
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

    def __repr__(self):
        ret_str = "<NestedIns ident:%s graph:%s end:%s state:%s>" \
                  % (self.ident, self.graph.name, self.is_end, self.state)
        return ret_str

    def __str__(self):
        ret_str = ">>------------\n"
        ret_str += super(NestedInstance, self).__str__()

        p = self.from_pace
        while p:
            ret_str += "\n%s" % p.sub_instance
            if p is self.to_pace:
                break
            p = p.nxt
        return ret_str
