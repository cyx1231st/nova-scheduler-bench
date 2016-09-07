import abc

from openstack_bench.os_patcher import patching as bench_patching


_UNDFINED = "UNDEFINED"


def printerror(_, error_str):
    print(error_str)


class BasePatcher(object):
    __metaclass__ = abc.ABCMeta

    REPOSITORY = _UNDFINED
    PATCH_POINT = _UNDFINED
    CONF = _UNDFINED

    def __init__(self, release):
        self.release = release

    def stub_entrypoint(self, patch_func):
        if self.PATCH_POINT == _UNDFINED:
            raise RuntimeError("Undefined patch entrypoint!")
        bench_patching.AopPatch.logger = printerror
        bench_patching.AopPatch(
            self.PATCH_POINT,
            after=lambda *args, **kwargs: patch_func())

    @abc.abstractmethod
    def stub_out_modules(self):
        """ Stub out repository modules """

    @abc.abstractmethod
    def override_configurations(self):
        """ Stub out repository configurations """

    def patch(self, name, attr, add=False):
        """ Patch module name with attr """
        bench_patching.MonkeyPatch(name, attr, add=add)

    def conf(self, name, val, group=None):
        self.CONF.set_override(name, val, group)
