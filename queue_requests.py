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

from __future__ import print_function

import argparse
import logging
import math
import random

import const

DEFAULT_RAM_MB_PER_NODE = 64 * 1024
DEFAULT_CPU_CORES_PER_NODE = 8
DEFAULT_DISK_GB_PER_NODE = 25 * 1024
DEFAULT_RAM_ALLOCATION_RATIO = 1.5
DEFAULT_CPU_ALLOCATION_RATIO = 16.0
DEFAULT_DISK_ALLOCATION_RATIO = 1.5

LOG = logging.getLogger('main')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('nodes', type=int,
                        help="Number of nodes running.")
    parser.add_argument('max', type=int,
                        help="Number of max requests.")
    parser.add_argument('--node-ram-mb', type=int,
                        default=DEFAULT_RAM_MB_PER_NODE,
                        help="Amount of RAM in MB per node.")
    parser.add_argument('--node-cpu-cores', type=int,
                        default=DEFAULT_CPU_CORES_PER_NODE,
                        help="Amount of physical CPU cores per node.")
    parser.add_argument('--node-disk-gb', type=int,
                        default=DEFAULT_DISK_GB_PER_NODE,
                        help="Amount of DISK in GB per node.")
    parser.add_argument('--node-ram-allocation-ratio', type=float,
                        default=DEFAULT_RAM_ALLOCATION_RATIO,
                        help="The RAM allocation ratio for each node.")
    parser.add_argument('--node-cpu-allocation-ratio', type=float,
                        default=DEFAULT_CPU_ALLOCATION_RATIO,
                        help="The CPU allocation ratio for each node.")
    parser.add_argument('--node-disk-allocation-ratio', type=float,
                        default=DEFAULT_DISK_ALLOCATION_RATIO,
                        help="The DISK allocation ratio for each node.")
    parser.add_argument('--out-file', default=".requests",
                        help="The file to store requests")
    args = parser.parse_args()
    queue_requests(args)


def queue_requests(args):
    ram_mb = args.node_ram_mb
    ram_allocation_ratio = args.node_ram_allocation_ratio
    total_ram_per = ram_mb * ram_allocation_ratio
    num_nodes = args.nodes
    total_ram = int(math.floor(total_ram_per * num_nodes))
    # Add some fudging factor to ensure we always have more requests
    # than could possibly fit on all nodes.
    total_ram *= 1.1

    num_requests = 0
    requests = []
    while total_ram > 0:
        res_tpl = random.choice(const.RESOURCE_TEMPLATES)
        total_ram -= res_tpl[const.RAM_MB]
        if total_ram > 0:
            requests.append(res_tpl[const.FID])
            num_requests += 1
            if num_requests >= args.max:
                break

    with open(args.out_file, 'w') as outfile:
        outfile.write("request_list=( ")
        for req in requests:
            outfile.write(str(req) + " ")
        outfile.write(")")
    print("Wrote instance requests to output file %s." %
             args.out_file)

    print("Placed %d instance requests into queue." % num_requests)
    

if __name__ == '__main__':
    main()
