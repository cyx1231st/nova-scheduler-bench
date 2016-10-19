import collections
import os
from os import path

# TODO: extract the logics of emitting and parsing logging fields (low
# priority)


class LogLine(object):
    def __init__(self, line, log_file):
        # line, filename
        self.filename = log_file.name
        self.line = line

        pieces = line.split()

        # seconds
        self.time = pieces[1]
        time_pieces = pieces[1].split(":")
        self.seconds = int(time_pieces[0]) * 3600 + \
            int(time_pieces[1]) * 60 + \
            float(time_pieces[2])

        # service, host
        pieces7 = None
        index = 0
        for piece in pieces:
            if piece.startswith("BENCH-"):
                pieces7 = piece
                break
            index += 1
        pieces7 = pieces7.split('-')
        host_pieces = pieces7[2:]
        host_pieces[-1] = pieces7[-1][:-1]
        self.host = '-'.join(host_pieces)
        self.service = pieces7[1]

        # instance_id, instance_name
        instance_info = pieces[index+1]
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

        # request_id, action
        self.request_id = pieces[index-3][5:]
        self.action = " ".join(pieces[index+2:])

        # validations
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

        # correct, ident
        self.correct = True
        self.ident = None

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
            if ret is not True and len(ret) == 36:
                mismatch_errors.add(ret)
                # self.log_lines[i].correct = False
        return mismatch_errors

    def catg_logs(self, name_errors, mismatch_errors):
        for log in self.log_lines:
            if log.correct and log.instance_name not in name_errors \
                    and log.instance_id not in mismatch_errors:
                    # and log.instance_name not in mismatch_errors \
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
    def __init__(self, log_folder, driver_obj):
        self.api = {}
        self.conductor = {}
        self.schedulers = {}
        self.computes = {}
        self.log_files = []

        self.relation = {}

        # current_path = path.dirname(os.path.realpath(__file__))
        current_path = os.getcwd()
        log_folder = path.join(current_path, log_folder)

        for f in os.listdir(log_folder):
            file_dir = path.join(log_folder, f)
            if not path.isfile(file_dir):
                continue
            if not f.endswith(".log"):
                continue
            if f.startswith("out"):
                continue
            if not f.startswith("BENCH"):
                continue
            f = LogFile(f, file_dir, self.relation)
            if f.service == "api":
                if f.host not in self.api:
                    self.api[f.host] = f
                    self.log_files.append(f)
                else:
                    raise RuntimeError("There's already a log for api: %s, "
                                       "but there is another one: %s"
                                       % (self.api[f.host].name, f.name))

            elif f.service == "conductor":
                if f.host not in self.conductor:
                    self.conductor[f.host] = f
                    self.log_files.append(f)
                else:
                    raise RuntimeError("There's already a log for conductor: "
                                       "%s, but there is another one: %s"
                                       % (self.conductor[f.host].name, f.name))
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
            mismatch_errors = mismatch_errors.union(lf.apply_relation(self.relation,
                                                                      relation_name))

        for lf in self.log_files:
            lf.catg_logs(name_errors, mismatch_errors)

        return name_errors, mismatch_errors

    def emit_logs(self):
        controller_logs = collections.defaultdict(list)
        compute_logs = self.computes

        active_schedulers, active_computes = 0, 0

        for api in self.api.values():
            for key, values in api.logs_by_ins.items():
                controller_logs[key].extend(values)
        for cond in self.conductor.values():
            for key, values in cond.logs_by_ins.items():
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
