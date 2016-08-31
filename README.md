# OpenStack Benchmarking for Scheduling

This tool is for benchmarking schedule performance using real OpenStack
deployment, in the most accurate and convenient approach.

The "accurate" means this tool leverages real OpenStack environment installed
in the system and introduces minimal overhead to the benched OpenStack. It does
make some changes to the installed OpenStack by doing monkey patches at
runtime, but only for injecting pointcuts to add loggings and providing
"convenience" for benchmarking without modifying logics during scheduling.

The "convenient" means this tool is ready for use whenever there is a runnable
OpenStack environment. No matter how this OpenStack or the underlying database
and message queue are configured for optimization, this tool is able to provide
the accurate and comprehensive performance report of scheduling in the current
deployment. During each run, the number of schedulers, number of compute
nodes and the number of requests can be changed very easily by modifying the
parameters of the benchmarking commands. Note that all the schedule services
and compute nodes are launched for real during benchmarking.

The benchmark report includes the schedule throughput, the race condition and
the accurate time cost of each phase in the entire schedule lifecycle, based on
parsing logs after a real run. The parsing program initiates state machines for
every recorded schedule requests. Each state machine emulates a recorded
request from it is received by nova-api to it is finally succeed or failed in
nova-compute or nova-conductor. So any retries, failures or success and their
time cost in every schedule phases are collected during parsing to generate the
final benchmark report.

## Installation

First, make sure the system has a runnable OpenStack environment. The required
services are nova-api, nova-conductor, nova-scheduler and nova-compute. Test
this environment by successfully spawning a VM and then delete it.

The simplest way to get a suitable OpenStack environment is to follow the
DevStack All-In-One Single Machine installation:
```
http://docs.openstack.org/developer/devstack/guides/single-machine.html
```

Second, switch to stack user and clone this repo to any folder you want.
```
$ git clone git@github.com:cyx1231st/nova-scheduler-bench.git
$ cd nova-scheduler-bench
```

The benchmark tool is ready for use now!

## Run scheduler bench

Run the following 6 bash scripts to do benchmarking.

### 0-prepare.sh

This command is to prepare the installed OpenStack environment before the 1st
benchmarking. It unleashes the quotas for admin user and adds flavors for
creating instances.

### 1-setup.sh

Kill all the services and launch the monkey-patched services instead. Use
parameters to define the number of schedulers, the number of compute nodes and
the max number of requests for benchmarking.
```
setup.sh <number of schedulers> <number of nodes> <number of max requests>
```

Then make sure the services are up after running this command.
```
$nova service-list
```

### 2-create.sh

Send requests to nova based on the generated flavors from "1-setup.sh". The
runtime logs are collected in folder
"./openstack_bench/openstack_patcher/results/". Make sure all the requests are
completely handled by nova. Here is an example benchmarking run using 2
schedulers, 5 compute nodes and sending 50 concurrent requests.
```
$ ./2-create.sh
python parse.py ./openstack_bench/openstack_patcher/results --brief

 >> LOG SUMMARY
Active schedulers: 2
Active computes: 5
Total requests count: 50
  Success requests: 50
```

### 3-report.sh

See the complete report by parsing the logs in
"./openstack_bench/openstack_patcher/results/" and move them to "./results/".
For example:
```
$ ./3-report.sh
successful requests:       48
nvh requests:              2
rtf requests:              0
api failed requests:       0
incomplete requests:       0
error requests:            0

total valid queries:       59
direct successful queries: 47
direct nvh queries:        0
direct retried queries:    3
retry successful queries:  1
retry nvh queries:         2
retry retried queires:     6

wall clock total(s):       12.26700
wall clock api:            2.80100
wall clock conductor:      2.76400
wall clock scheduler:      10.37700
wall clock compute:        3.03600
wall clock query:          8.86734

time inapi avg:            1.97620
time a-con avg:            0.00872
time cond1 avg:            0.00682
time c-sch avg:            0.27352
time sched avg:            6.35410
time s-con avg:            0.00656
time cond2 avg:            0.09132
time c-com avg:            0.01116
time compu avg:            0.20816

time filter avg:           6.35170
time cache refresh avg:    6.34028
time gap avg:              0.32944

percent api part:          22.11365
percent msg part:          3.35655
percent cond part:         1.09819
percent sch part:          71.10230
percent compute part:      2.32931
percent filter part:       71.07545
percent cache-refresh part:70.94766
percent gap part:          3.68643

request per sec:           4.07598
query per sec:             4.80965
success per sec:           3.91294

percent query retry:       18.75000
percent request api fail:  0.00000
Done!
```

### 4-delete.sh

Remove all instances created in the OpenStack.
Make sure all instances are deleted by executing:
```
$ nova list
```

### 5-reset.sh

Kill all the monkey-patched services.

### Use cases

1. Run commands from #1 to #5 to execute complete benchmark for the specified
schedulers, compute nodes and requests settings. Then run #1 to launch another
set of benchmark.

2. Run commands from #2 to #4 repeatly to run benchmark at the same settings of
schedulers, nodes and requests.
