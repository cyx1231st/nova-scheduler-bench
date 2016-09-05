import abc

from openstack_bench.os_patcher import patching as bench_patching


_UNDFINED = "UNDEFINED"


def printerror(error_str):
    print(error_str)


class BasePatcher(object):
    __metaclass__ = abc.ABCMeta

    REPOSITORY = _UNDFINED
    PATCH_POINT = _UNDFINED

    def __init__(self):
        pass

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
