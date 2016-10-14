import abc

from oslo_log import log as logging

from openstack_bench.os_patcher import patching as bench_patching


LOG = logging.getLogger(__name__)
_UNDFINED = "UNDEFINED"


def printerror(error_str):
    print(error_str)


class BasePatcher(object):
    """ BasePatcher for OpenStack projects """

    __metaclass__ = abc.ABCMeta

    REPOSITORY = _UNDFINED
    PATCH_POINT = _UNDFINED
    CONF = _UNDFINED

    def __init__(self, service_name, host_name, release=None):
        self.release = release
        self.service_name = service_name
        self.host_name = host_name
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
    def _override_configurations(self):
        """ Stub out repository configurations """

    def override_configurations(self, is_console, is_debug, folder):
        """ Stub out repository configurations """
        self.conf("host", self.host_name)

        # Enable console mode
        if not is_console:
            self.conf(
                "logging_default_format_string",
                "%(asctime)s.%(msecs)03d %(levelname)s %(name)s [-] "
                "%(instance)s%(message)s")
            self.conf(
                "logging_debug_format_suffix",
                "from (pid=%(process)d) %(funcName)s "
                "%(pathname)s:%(lineno)d")
            self.conf(
                "logging_exception_prefix",
                "%(asctime)s.%(msecs)03d TRACE %(name)s %(instance)s")
            self.conf(
                "logging_context_format_string",
                "%(asctime)s.%(msecs)03d %(levelname)s %(name)s "
                "[%(request_id)s %(user_name)s %(project_name)s] "
                "%(instance)s%(message)s")
            self.conf(
                "log_file",
                folder + "BENCH-" + self.service_name + "-"
                + self.host_name + ".log")

        # Enable debug mode
        self.conf("debug", is_debug)

        # Custom overriding
        self._override_configurations()

    def inject_logs(self, points, engine):
        bench_patching.AopPatch.logger = staticmethod(self.error)
        bench_patching.AopPatch.printer = staticmethod(self.printer)

        # TODO: report status
        bench_patching.AopPatch(
            "oslo_log.log.setup",
            after=lambda arg: "Bench initiated %s!" % engine.subvirted)

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

    # helper methods
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
