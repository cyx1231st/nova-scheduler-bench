import abc

from oslo_log import log as logging

from openstack_bench.os_patcher import patching as bench_patching


LOG = logging.getLogger(__name__)
_UNDFINED = "UNDEFINED"


def printerror(error_str):
    print(error_str)


class BasePatcher(object):
    __metaclass__ = abc.ABCMeta

    REPOSITORY = _UNDFINED
    PATCH_POINT = _UNDFINED
    CONF = _UNDFINED

    def __init__(self, service_name, host_name, release=None):
        self.release = release
        self.service_name = service_name
        self.log_prefix = "BENCH-" + service_name + "-" + host_name + ": "

    def stub_entrypoint(self, patch_func):
        if self.PATCH_POINT == _UNDFINED:
            raise RuntimeError("Undefined patch entrypoint!")
        bench_patching.AopPatch.logger = staticmethod(printerror)
        bench_patching.AopPatch(
            self.PATCH_POINT,
            after=lambda arg: patch_func(),
            direct=True)

    @abc.abstractmethod
    def stub_out_modules(self):
        """ Stub out repository modules """

    @abc.abstractmethod
    def override_configurations(self):
        """ Stub out repository configurations """

    def inject_logs(self, points):
        bench_patching.AopPatch.logger = staticmethod(self.error)
        bench_patching.AopPatch.printer = staticmethod(self.printer)

        bench_patching.AopPatch(
            "oslo_log.log.setup",
            after=lambda arg: "Bench initiated!")

        for point in points:
            if point.project != self.REPOSITORY:
                raise RuntimeError("Project don't match: %s, %s"
                                   % (point.project, self.REPOSITORY))

            if self.release in point:
                i_point = point[self.release]
            elif None in point:
                i_point = point[None]
            else:
                self.printer("Skip point %s" % point.inject_point)
                continue
            try:
                bench_patching.AopPatch(point.inject_point,
                                        before=i_point.before,
                                        after=i_point.after,
                                        excep=i_point.excep)
                self.printer("Load point %s" % point.inject_point)
            except Exception:
                self.printer("Failed to load %s!" % point.inject_point)

    def patch(self, name, attr, add=False):
        """ Patch module name with attr """
        bench_patching.MonkeyPatch(name, attr, add=add)

    def conf(self, name, val, group=None):
        self.CONF.set_override(name, val, group)

    # loggers
    def printer(self, msg):
        LOG.warn(self.log_prefix + msg)

    def error(self, msg):
        LOG.error(self.log_prefix + msg)
