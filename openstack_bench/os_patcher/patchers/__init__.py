from nova_patcher import NovaPatcher


PATCHERS = [NovaPatcher]


def available_services():
    names = set()
    for patcher in PATCHERS:
        for name in patcher.SERVICES:
            if name in names:
                raise RuntimeError("Duplicated name %s!" % name)
            names.add(name)
    return list(names)


def load_patcher(service_name, host_name):
    for patcher in PATCHERS:
        if service_name in patcher.SERVICES:
            print("Loading service %s from patcher %s..."
                  % (service_name, patcher.REPOSITORY))
            return patcher(service_name, host_name)
    raise RuntimeError("Cannot load patcher, cannot find service name %s"
                       % service_name)
