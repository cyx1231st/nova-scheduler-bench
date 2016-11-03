from collections import defaultdict

from state_machine import LeafInstance
from state_machine import NestedInstance
from state_machine import ParseError


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

                            if not ins.confirm(log):
                                raise RuntimeError(
                                    "Unrecognized init_log:%s" % log)

                            helper.add(ins)

        for helper in helpers_by_ident.values():
            helper.sort()

        class BreakIt(Exception):
            pass

        # step 2: build nested instances
        instances = {}
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

            instances[ident] = instance

        return instances
