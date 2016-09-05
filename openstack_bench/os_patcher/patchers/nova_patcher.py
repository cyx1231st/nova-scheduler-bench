from nova.virt import fake

import bases


class SchedulerFakeDriver(fake.FakeDriver):
    vcpus = 4
    memory_mb = 8192
    local_gb = 1024

    @classmethod
    def prepare_resource(cls, resource):
        if resource:
            cls.vcpus = resource.vcpus
            cls.memory_mb = resource.memory_mb
            cls.local_gb = resource.local_gb

    def get_available_resource(self, nodename):
        host_state = super(SchedulerFakeDriver, self)\
            .get_available_resource(nodename)
        host_state['disk_available_least'] = \
            host_state['local_gb'] - host_state['local_gb_used']
        return host_state


class NovaPatcher(bases.BasePatcher):
    REPOSITORY = "nova"
    PATCH_POINT = "nova.config.parse_args"

    def __init__(self):
        super(NovaPatcher, self).__init__()

    def stub_out_modules(self):
        pass
