from oslo_config import cfg
from oslo_log import log as logging

import openstack_bench.os_patcher.patching as bench_patching


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


AVAILABLE_DRIVERS = {}


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
        bench_patching.AopPatch.logger = self.error
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

    def error(self, msg):
        LOG.error(self.meta.log_prefix + msg)


def register_driver(name, driver):
    if name in AVAILABLE_DRIVERS:
        raise RuntimeError("Conflicted driver name: %s" % name)
    AVAILABLE_DRIVERS[name] = driver
