from nova_patcher import NovaPatcher


PATCHERS = [NovaPatcher]


def load_patcher(service_name, host_name):
    for patcher in PATCHERS:
        if service_name in patcher.SERVICES:
            print("Loading %s from project %s..."
                  % (service_name, patcher.REPOSITORY))
            return patcher(service_name, host_name)
    raise RuntimeError("Cannot load patcher, cannot find service name %s"
                       % service_name)
