from openstack_bench.config import CONF_BENCH
from openstack_bench import bench_drivers
from patchers import load_patcher


def printerror(error_str):
    print(error_str)


class PatchEngine(object):
    def __init__(self, args):
        self.args = args

        self.patcher = load_patcher(args.service, args.host)

        driver_class_name = CONF_BENCH.bench_driver
        self.driver_obj = bench_drivers.init_driver(driver_class_name)
        self.driver_obj.release = self.patcher.release

        self.subvirted = False

    def _apply_patch(self):
        if self.subvirted:
            raise RuntimeError("Already subvirted!")
        self.patcher.printer("Patching...")

        # Patch repository specific modules
        self.patcher.stub_out_modules()

        # Patch repository specific configurations
        self.patcher.override_configurations(self.args.console,
                                             self.args.debug,
                                             self.args.result_folder)

        # Inject logs dynamically
        points = self.driver_obj.points.values()
        self.patcher.inject_logs(points, self)

        self.patcher.printer("Patching Success!")

        self.subvirted = True

    def subvirt(self):
        self.patcher.stub_entrypoint(self._apply_patch)
        self.patcher.run_service()
