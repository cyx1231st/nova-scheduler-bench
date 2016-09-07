import enum


class SchedulerType(enum.Enum):
    FILTER = ("host_manager", "filter_scheduler")
    CACHING = ("host_manager", "caching_scheduler")
    SHARED = ("shared_host_manager", "filter_scheduler")
DEFAULT_SCHEDULER_TYPE = SchedulerType.FILTER
