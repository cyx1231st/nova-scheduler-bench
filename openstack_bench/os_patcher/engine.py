import sys

from nova.cmd import api
from nova.cmd import compute
from nova.cmd import conductor
from nova.cmd import scheduler

import patching as bench_patching


def printerror(error_str):
    print(error_str)


class PatchEngine(object):
    def __init__(self, meta, driver_obj):
        self.meta = meta
        self.driver_obj = driver_obj

    def _apply_patch(self):
        self.driver_obj.stubout_nova()
        self.driver_obj.stubout_conf()
        self.driver_obj.inject_logs()

    def subvirt(self, service_name):
        bench_patching.AopPatch.logger = printerror
        bench_patching.AopPatch(
            "nova.config.parse_args",
            after=lambda *args, **kwargs: self._apply_patch())

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
