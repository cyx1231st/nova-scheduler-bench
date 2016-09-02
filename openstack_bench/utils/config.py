import os
from os import path
from oslo_config import cfg


_file_path = path.dirname(os.path.realpath(__file__))
_DIR = path.join(_file_path, "../bench.conf")

CONF_BENCH = cfg.ConfigOpts()

_default_opts = [
    cfg.StrOpt("bench_driver",
               # TODO: change to choices
               default="driver_scheduler"),
]

CONF_BENCH.register_opts(_default_opts)
CONF_BENCH(['--config-file', _DIR], project="bench")
