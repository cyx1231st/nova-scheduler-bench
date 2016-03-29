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
    parser.add_argument("service",
                        help="Select the nova service.")
    args = parser.parse_args()
    bench.init(args)


if __name__ == "__main__":
    main()
