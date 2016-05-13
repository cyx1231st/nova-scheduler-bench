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

import argparse

import bench


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action="store_true",
                        help="Show debug logs during run.")
    parser.add_argument('--console', action="store_true",
                        help="Colored console logs during run without "
                             "log files, used for screen.")
    parser.add_argument('--host',
                        default=bench.BenchmarkMeta.DEFAULT_HOST,
                        help="If set, service will be started using this "
                             "hostname instead of machine name. Used for "
                             "start parallel services in the same host.")
    parser.add_argument('--driver',
                        default=bench.BenchmarkMeta.DEFAULT_DRIVER,
                        help="If set, the benchmark driver will be changed, "
                             "the default is driver_scheduler.")
    parser.add_argument('--result-folder',
                        default=".",
                        help="If set, the logs will be in that folder.")
    parser.add_argument('--view',
                        default="A",
                        help="The resource type loaded, A is normal, "
                        "B cannot hold any vm of flavor 154, "
                        "and C can only hold 1 vm of flavor 154.")
    parser.add_argument('--new', action="store_true",
                        help="The new code has a different inject point.")
    parser.add_argument('--scheduler-type',
                        default="shared",
                        help="filter: filter scheduler; "
                        "caching: caching scheduler; "
                        "shared: shared-state scheduler.")
    parser.add_argument("service",
                        help="Launched nova service type.")
    args = parser.parse_args()
    bench.init(args)


if __name__ == "__main__":
    main()
