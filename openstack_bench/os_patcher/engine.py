import sys

from nova.cmd import api
from nova.cmd import compute
from nova.cmd import conductor
from nova.cmd import scheduler

from patchers.nova_patcher import NovaPatcher


def printerror(error_str):
    print(error_str)


class PatchEngine(object):
    def __init__(self, meta, driver_obj):
        self.meta = meta
        self.driver_obj = driver_obj
        # TODO: build patcher from driver
        self.patcher = NovaPatcher()

    def _apply_patch(self):
        self.patcher.stub_out_modules()

        # Replace host if DEFAULT_HOST not set
        if self.meta.host and self.meta.host != self.meta.DEFAULT_HOST:
            self.patcher.conf("host", self.meta.host)

        # Enable console mode
        if not self.meta.is_console:
            self.patcher.conf(
                "logging_default_format_string",
                "%(asctime)s.%(msecs)03d %(levelname)s %(name)s [-] "
                "%(instance)s%(message)s")
            self.patcher.conf(
                "logging_debug_format_suffix",
                "from (pid=%(process)d) %(funcName)s "
                "%(pathname)s:%(lineno)d")
            self.patcher.conf(
                "logging_exception_prefix",
                "%(asctime)s.%(msecs)03d TRACE %(name)s %(instance)s")
            self.patcher.conf(
                "logging_context_format_string",
                "%(asctime)s.%(msecs)03d %(levelname)s %(name)s "
                "[%(request_id)s %(user_name)s %(project_name)s] "
                "%(instance)s%(message)s")
            self.patcher.conf(
                "log_file",
                self.meta.folder + "BENCH-" + self.meta.service + "-"
                + self.meta.host + ".log")

        # Enable debug mode
        self.patcher.conf("debug", self.meta.is_debug)

        # self.driver_obj.stubout_conf()
        self.patcher.override_configurations()

        self.driver_obj.inject_logs()

    def subvirt(self, service_name):
        self.patcher.stub_entrypoint(self._apply_patch)

        sys.argv = [""]
        sys.argv.append("--config-file")
        sys.argv.append("/etc/nova/nova.conf")
        service = service_name
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
            raise RuntimeError("Unsupported service %s" % service)
