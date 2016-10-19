# Copyright (c) 2016 Yingxin Cheng
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from collections import OrderedDict


_AVAILABLE_DRIVERS = OrderedDict()


def register_driver(name, driver):
    if name in _AVAILABLE_DRIVERS:
        raise RuntimeError("Conflicted driver name: %s" % name)
    _AVAILABLE_DRIVERS[name] = driver


def get_driver_names():
    return _AVAILABLE_DRIVERS.keys()


def init_driver(name):
    return _AVAILABLE_DRIVERS[name]()


def from_config():
    from openstack_bench.config import CONF_BENCH

    driver_class_name = CONF_BENCH.bench_driver
    driver_obj = init_driver(driver_class_name)
    return driver_obj


def _register_all():
    # NOTE(Yingxin): The first driver will be the default driver.
    __import__("openstack_bench.bench_drivers.driver_scheduler")
_register_all()


DEFAULT_DRIVER = _AVAILABLE_DRIVERS.keys()[0]
