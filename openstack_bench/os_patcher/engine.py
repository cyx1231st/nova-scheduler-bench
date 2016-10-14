from patchers import load_patcher


def printerror(error_str):
    print(error_str)


class PatchEngine(object):
    def __init__(self, meta, driver_obj, service_name):
        self.meta = meta
        self.driver_obj = driver_obj

        self.service_name = service_name

        self.patcher = load_patcher(self.service_name, self.meta.host)
        self.driver_obj.release = self.patcher.release
        self.subvirted = False

    def _apply_patch(self):
        if self.subvirted:
            raise RuntimeError("Already subvirted!")
        self.subvirted = True

        self.patcher.stub_out_modules()

        self.patcher.conf("host", self.meta.host)

        # Enable console mode
        # TODO: move it to BasePatcher
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

        # Inject logs dynamically
        points = self.driver_obj.points.values()
        self.patcher.inject_logs(points)

    def subvirt(self):
        self.patcher.stub_entrypoint(self._apply_patch)
        self.patcher.run_service()
