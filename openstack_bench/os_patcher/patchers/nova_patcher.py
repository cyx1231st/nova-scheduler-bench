import sys

from nova.cmd import api
from nova.cmd import compute
from nova.cmd import conductor
from nova.cmd import scheduler
from nova.network import model as network_model
from nova.virt import fake

from oslo_config import cfg

from consts import SchedulerType
import bases
from openstack_bench.config import CONF_BENCH
from openstack_bench.releases import Release


class _SchedulerFakeDriver(fake.FakeDriver):
    vcpus = CONF_BENCH.nova_patcher.vcpus
    memory_mb = CONF_BENCH.nova_patcher.memory_mb
    local_gb = CONF_BENCH.nova_patcher.disk_gb

    def get_available_resource(self, nodename):
        host_state = super(_SchedulerFakeDriver, self)\
            .get_available_resource(nodename)
        host_state['disk_available_least'] = \
            host_state['local_gb'] - host_state['local_gb_used']
        return host_state


class NovaPatcher(bases.BasePatcher):
    REPOSITORY = "nova"
    PATCH_POINT = "nova.config.parse_args"
    CONF = cfg.CONF

    def __init__(self):
        release = Release[CONF_BENCH.nova_patcher.release]
        super(NovaPatcher, self).__init__(release)

    def stub_out_modules(self):
        # NOTE: if simulation mode is enabled, the virt driver will be replaced
        # by a fake driver.
        if CONF_BENCH.nova_patcher.enable_simulation:
            self.patch('nova.virt.fake.SchedulerFakeDriver',
                       _SchedulerFakeDriver,
                       add=True)

            fake_async_networkinfo = \
                lambda *args, **kwargs: network_model.NetworkInfoAsyncWrapper(
                    lambda *args, **kwargs: network_model.NetworkInfo())
            fake_deallocate_networkinfo = lambda *args, **kwargs: None
            fake_check_requested_networks = lambda *args, **kwargs: 1
            self.patch('nova.compute.manager.ComputeManager._allocate_network',
                       fake_async_networkinfo)
            self.patch('nova.compute.manager.ComputeManager._deallocate_network',
                       fake_deallocate_networkinfo)
            self.patch('nova.compute.api.API._check_requested_networks',
                       fake_check_requested_networks)

    def override_configurations(self):
        if CONF_BENCH.nova_patcher.enable_simulation:
            if self.release == Release.LATEST:
                self.conf("compute_driver",
                          'fake.SchedulerFakeDriver')
            else:
                self.conf("compute_driver",
                          'nova.virt.fake.SchedulerFakeDriver')

            if self.release != Release.KILO:
                self.conf("ram_allocation_ratio",
                          CONF_BENCH.nova_patcher.ram_allocation_ratio)
                self.conf("cpu_allocation_ratio",
                          CONF_BENCH.nova_patcher.cpu_allocation_ratio)
                self.conf("disk_allocation_ratio",
                          CONF_BENCH.nova_patcher.disk_allocation_ratio)

                scheduler_t = CONF_BENCH.nova_patcher.scheduler_type
                scheduler_t = SchedulerType[scheduler_t]
                self.conf("scheduler_driver",
                          scheduler_t.value[1])
                self.conf("scheduler_host_manager",
                          scheduler_t.value[0])

            self.conf("reserved_host_disk_mb", 0)
            self.conf("reserved_host_memory_mb", 0)
            self.conf("scheduler_max_attempts", 5)
            self.conf("scheduler_default_filters",
                      ["RetryFilter",
                       "AvailabilityZoneFilter",
                       "RamFilter",
                       "DiskFilter",
                       # "CoreFilter",
                       "ComputeFilter",
                       "ComputeCapabilitiesFilter",
                       "ImagePropertiesFilter",
                       "ServerGroupAntiAffinityFilter",
                       "ServerGroupAffinityFilter"])

    def run_service(self, service_name):
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
