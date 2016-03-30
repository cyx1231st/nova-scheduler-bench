import sys

from oslo_config import cfg
from oslo_log import log as logging

from nova.cmd import api
from nova.cmd import compute
from nova.cmd import conductor
from nova.cmd import scheduler

import patching as bench_patching


CONF = cfg.CONF
LOG = logging.getLogger(__name__)

BENCH = None


class BenchmarkMeta(object):
    DEFAULT_HOST = "_local_"
    DEFAULT_DRIVER = "driver_scheduler"

    def __init__(self, args):
        self.enabled = True
        self.host = args.host
        self.is_debug = args.debug
        self.is_console = args.console
        self.service = args.service
        self.log_prefix = "BENCH-" + self.service + "-" + self.host + ": "
        self.folder = args.result_folder + "/"
        """
        driver_name = args.driver
        try:
            module = __import__(driver_name, globals(), level=1)
            self.driver = module.get_driver(self)
            if not isinstance(self.driver, BenchDriverBase):
                raise RuntimeError()
        except Exception:
            self.enabled = False
            raise
        """
        import driver_scheduler
        self.driver = driver_scheduler.get_driver(self)


class BenchDriverBase(object):
    def __init__(self, meta):
        self.meta = meta

    def _stubout_nova(self):
        raise NotImplementedError()

    def _stubout_conf(self):
        raise NotImplementedError()

    def _inject_logs(self):
        raise NotImplementedError()

    def stubout_nova(self):
        self._stubout_nova()

    def stubout_conf(self):
        if self.meta.host and self.meta.host != self.meta.DEFAULT_HOST:
            self.conf("host", self.meta.host)
        if not self.meta.is_console:
            self.conf("logging_default_format_string",
                      "%(asctime)s.%(msecs)03d %(levelname)s %(name)s [-] "
                      "%(instance)s%(message)s")
            self.conf("logging_debug_format_suffix",
                      "from (pid=%(process)d) %(funcName)s "
                      "%(pathname)s:%(lineno)d")
            self.conf("logging_exception_prefix",
                      "%(asctime)s.%(msecs)03d TRACE %(name)s %(instance)s")
            self.conf("logging_context_format_string",
                      "%(asctime)s.%(msecs)03d %(levelname)s %(name)s "
                      "[%(request_id)s %(user_name)s %(project_name)s] "
                      "%(instance)s%(message)s")
            self.conf("log_file",
                      self.meta.folder + "BENCH-" + self.meta.service + "-"
                      + self.meta.host + ".log")
        self.conf("debug", self.meta.is_debug)
        self._stubout_conf()

    def inject_logs(self):
        self.patch_aop(
            "oslo_log.log.setup",
            after=lambda *args, **kwargs:
                self.warn("Bench initiated!"))
        self._inject_logs()

    # patchings
    def patch(self, name, attr, add=False):
        bench_patching.MonkeyPatch(name, attr, add=add)

    def patch_aop(self, name, before=None, after=None):
        bench_patching.AopPatch(name, before=before, after=after)

    def conf(self, name, val, group=None):
        CONF.set_override(name, val, group)

    # loggers
    def info(self, msg):
        LOG.info(self.meta.log_prefix + msg)

    def debug(self, msg):
        LOG.debug(self.meta.log_prefix + msg)

    def warn(self, msg):
        LOG.warn(self.meta.log_prefix + msg)


def init(args):
    bench_patching.AopPatch(
        "nova.config.parse_args",
        after=lambda *args, **kwargs: patch_nova())

    global BENCH
    BENCH = BenchmarkMeta(args)

    sys.argv = [""]
    sys.argv.append("--config-file")
    sys.argv.append("/etc/nova/nova.conf")
    service = args.service
    if service == "compute":
        sys.argv[0] = "nova-compute"
        compute.main()
    elif service == "conductor":
        sys.argv[0] = "nova-conductor"
        conductor.main()
    elif service == "scheduler":
        sys.argv[0] = "nova-scheduler"
        scheduler.main()
    elif service == "api":
        sys.argv[0] = "nova-api"
        api.main()
    else:
        print "Unsupported service %s" % service


def patch_nova():
    if not BENCH or not BENCH.enabled:
        return

    BENCH.driver.stubout_nova()
    BENCH.driver.stubout_conf()
    BENCH.driver.inject_logs()
