from orderedset import OrderedSet


class Node(object):
    def __init__(self, id_, master_graph):
        self.id_ = id_
        self.master_graph = master_graph
        self.edges = OrderedSet()
        self.state = None

        # determine ownership
        self.determined = False
        self.graph = None

    def __repr__(self):
        if self.determined:
            if self.graph:
                if self.state:
                    return "<Node#%s: %s %r>" \
                           % (self.id_, self.graph.name, self.state)
                else:
                    return "<Node#%s: %s>" % (self.id_, self.graph.name)
            else:
                return "<!Node#%s: DETERMINE NONE!>" % self.id_
        elif self.determined is False:
            if self.graph is self.master_graph:
                return "<!Node#%s: GUESS MASTER!>" % self.id_
            elif self.graph:
                if self.state:
                    return "<Node#%s: ?%s %r>" \
                           % (self.id_, self.graph.name, self.state)
                else:
                    return "<Node#%s: ?%s>" % (self.id_, self.graph.name)
            else:
                return "<!Node#%s: NEW!>" % self.id_

    @property
    def correct(self):
        # Allowed combinations:
        # 1. determined = True, graph is master
        # 2. determined = True, graph is not master
        # 3. determined = False, graph is not master
        if self.determined and self.graph is not None:
            return True
        if self.determined is False and isinstance(self.graph, LeafGraph):
            return True
        print("%s is not correct!" % self)
        return False

    @property
    def guessed(self):
        return self.determined is False and self.graph is not None

    @property
    def determined_master(self):
        return self.determined and self.graph is self.master_graph

    @property
    def is_new(self):
        return self.determined is False and self.graph is None

    def add_edge(self, edge):
        # NOTE: Order matters
        self.edges.add(edge)

    def guess_graph(self, graph):
        assert graph is not self.master_graph
        if self.determined:
            pass
        elif self.graph is None:
            self.graph = graph
        elif self.graph is graph:
            pass
        else:
            self.determine_graph(self.master_graph)

    def determine_graph(self, graph):
        if not self.determined:
            if self.graph is graph:
                graph.determine_node(self)
            else:
                self.master_graph.determine_node(self)
        elif self.determined_master is False and self.graph is not graph:
            raise RuntimeError("Node#%s is determined %s, but assigned %s"
                               % (self, self.graph.name, graph.name))

    def decide_edge(self, log):
        for edge in self.edges:
            if edge.accept(log):
                return edge
        return None

    def accept_edge(self, edge):
        return edge in self.edges


class Edge(object):
    _sentinal = object()

    def __init__(self, node, graph, keyword):
        self.node = node
        self.graph = graph
        self.keyword = keyword

        self._assume_host = None

    @property
    def service(self):
        return self.graph.service

    def __repr__(self):
        if self.node:
            return "<Edge#%s -> Node#%s, %s>" \
                   % (self.service, self.node.id_, self.keyword)
        else:
            return "<!Edge#%s -> None, %s!>" % (self.service, self.keyword)

    def accept(self, log):
        return self.service == log.service and self.keyword in log.action

    @property
    def f_assume_host(self):
        return self._assume_host

    @f_assume_host.setter
    def f_assume_host(self, val):
        assert self.node in self.graph.end_nodes
        self._assume_host = val


class Graph(object):
    def __init__(self, name):
        self.name = name
        self.mid_nodes = set()
        self.start_nodes = set()
        self.end_nodes = set()
        self.tracked_nodes = set()

    def determine_node(self, node):
        if node.determined:
            raise RuntimeError("Node %s is already determined!" % node.id_)
        node.determined = True
        node.graph = self
        self.mid_nodes.add(node)

    def deal_start_end(self, from_node, to_node):
        if from_node in self.end_nodes:
            self.end_nodes.remove(from_node)
        elif from_node not in self.tracked_nodes:
            self.start_nodes.add(from_node)

        if to_node in self.start_nodes:
            self.start_nodes.remove(to_node)
        elif to_node not in self.tracked_nodes:
            self.end_nodes.add(to_node)
        self.tracked_nodes.add(from_node)
        self.tracked_nodes.add(to_node)

    def __repr__(self):
        included = set()
        ret_str = [""]
        ret_str[0] += "\nGraph %s:" % self.name

        def parse_node(node):
            if node in included:
                return
            included.add(node)
            if node in self.start_nodes:
                ret_str[0] += "\n+   %s" % node
            elif node in self.end_nodes:
                ret_str[0] += "\n-   %s" % node
                return
            else:
                ret_str[0] += "\n    %s" % node

            for edge in node.edges:
                ret_str[0] += "\n        %s" % edge
            for edge in node.edges:
                parse_node(edge.node)

        for start in self.start_nodes:
            parse_node(start)
        ret_str[0] += "\nTracked nodes: %s" % len(self.tracked_nodes)
        return ret_str[0]

    def decide_node_edge(self, log):
        for node in self.start_nodes:
            edge = node.decide_edge(log)
            if edge is not None:
                return node, edge
        return None, None

    def accept(self, log):
        node, edge = self.decide_node_edge(log)
        if node is not None and edge is not None:
            return True
        else:
            return False


class LeafGraph(Graph):
    def __init__(self, name, master):
        self.edges = set()
        # TODO: support fork and join
        self.ignored_edges = set()
        self.master_graph = master
        super(LeafGraph, self).__init__(name)

    @property
    def service(self):
        return self.name

    def add_edge(self, from_node, to_node, service_name, keyword):
        if service_name != self.service:
            raise RuntimeError("%s -> %s: Error adding edge %s -> graph %s!"
                               % (from_node, to_node,
                                  service_name, self.service))

        edge = Edge(to_node, self, keyword)
        self.edges.add(edge)
        from_node.add_edge(edge)

        self.deal_start_end(from_node, to_node)

    def add_ignore_edge(self, service_name, keyword):
        assert self.service == service_name
        edge = Edge(None, self, keyword)
        self.ignored_edges.add(edge)

    def merge(self, graph):
        if graph is self:
            return
        assert self.service == graph.service
        assert self.master_graph is graph.master_graph
        self.master_graph.remove_diagrem(graph)
        for node in graph.mid_nodes:
            self.mid_nodes.add(node)
        for node in graph.start_nodes:
            self.start_nodes.add(node)
        for node in graph.end_nodes:
            self.end_nodes.add(node)
        for node in graph.tracked_nodes:
            self.tracked_nodes.add(node)
            if node.graph is graph:
                node.graph = self
        for edge in graph.edges:
            self.edges.add(edge)
            edge.graph = self

    def __repr__(self):
        ret_str = ">>> LeafGraph"
        ret_str += super(LeafGraph, self).__repr__()
        if self.ignored_edges:
            ret_str += "\nIgnored edges:"
            for edge in self.ignored_edges:
                ret_str += "\n    %s" % edge
        ret_str += "\n"
        return ret_str

    def decide_edge_ignored(self, log):
        for edge in self.ignored_edges:
            if edge.accept(log):
                return edge
        return None


class MasterGraph(Graph):
    def __init__(self, name):
        self.tracked_nodes_by_id = {}
        self.graphs = set()
        super(MasterGraph, self).__init__(name)

    def create_graph(self, service_name):
        graph = LeafGraph(service_name, self)
        self.graphs.add(graph)
        return graph

    def remove_diagrem(self, graph):
        self.graphs.remove(graph)

    def get_edge(self, from_, to):
        from_node = self.tracked_nodes_by_id[from_]
        for edge in from_node.edges:
            if edge.node.id_ == to:
                return edge
        raise RuntimeError("Cannot find edge by %s -> %s"
                           % (from_, to))

    def get_graph(self, from_, to):
        return self.get_edge(from_, to).graph

    def set_state(self, node_id, state_str):
        node = self.tracked_nodes_by_id.get(node_id)
        if not node:
            raise RuntimeError("Cannot find node %s" % node_id)
        elif node not in self.end_nodes:
            raise RuntimeError("Cannot set state to node %s" % node)
        else:
            node.state = state_str

    def track_node(self, node_id):
        """ Get node from tracked nodes. """
        node = self.tracked_nodes_by_id.get(node_id)
        if node is None:
            node = Node(node_id, self)
            self.tracked_nodes_by_id[node_id] = node
        return node

    def build(self, from_, to, service_name, keyword):
        try:
            from_node = self.track_node(from_)
            if from_node.is_new:
                # Only one start node is allowed
                assert len(self.start_nodes) == 0
                from_node.determine_graph(self)
            assert from_node.correct

            to_node = self.track_node(to)
            if to_node.is_new:
                new_graph = self.create_graph(service_name)
                to_node.guess_graph(new_graph)
            assert to_node not in self.start_nodes
            assert to_node.correct

            target_graph = None
            if from_node.determined_master:
                if to_node.determined_master:
                    raise RuntimeError("Master's node %s cannot link to "
                                       "Master's node %s!"
                                       % (from_node, to_node))
            #     else:
            #         target_graph = to_node.graph
            # elif from_node.determined:
            #     target_graph = from_node.graph
            # else:  # from_node is guessed
            #     if to_node.determined_master:
            #         target_graph = from_node.graph
            #     elif to_node.determined:
            #         target_graph = to_node.graph
            #     elif to_node.guessed:
            #         if from_node.graph.service == service_name:
            #             target_graph = from_node.graph
            #         else:
            #             target_graph = to_node.graph

            from_graph = from_node.graph
            if isinstance(from_graph, LeafGraph) and \
                    from_graph.service == service_name:
                target_graph = from_graph
            else:
                target_graph = to_node.graph
            assert target_graph.service == service_name

            if from_node.graph.name == to_node.graph.name:
                from_node.graph.merge(to_node.graph)
                target_graph = from_node.graph
                to_node.graph = target_graph
            target_graph.add_edge(from_node, to_node, service_name, keyword)

            from_node.determine_graph(target_graph)
            to_node.guess_graph(target_graph)

            self.deal_start_end(from_node, to_node)
        except Exception:
            print("Building error: %s->%s, service %s, %s"
                  % (from_, to, service_name, keyword))
            for node in self.tracked_nodes_by_id.values():
                print(node)
            raise

    def decide_subgraph(self, log):
        for sub in self.graphs:
            if sub.accept(log):
                return sub
        return None

    def __repr__(self):
        ret_str = ">>> MasterGraph:"
        ret_str += super(MasterGraph, self).__repr__()
        ret_str += "\nSubgraphs: %s" % len(self.graphs)
        ret_str += "\n"
        return ret_str


def build_graph():
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
