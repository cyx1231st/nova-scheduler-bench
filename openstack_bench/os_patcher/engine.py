from patchers.nova_patcher import NovaPatcher


def printerror(error_str):
    print(error_str)


class PatchEngine(object):
    def __init__(self, meta, driver_obj, service_name):
        self.meta = meta
        self.driver_obj = driver_obj
        # TODO: service discovery and no alias
        self.service_name = service_name
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

        # Patch repository specific configurations
        self.patcher.override_configurations()

        # TODO: Inject logs using patcher
        self.driver_obj.inject_logs()

    def subvirt(self):
        self.patcher.stub_entrypoint(self._apply_patch)
        self.patcher.run_service(self.service_name)
