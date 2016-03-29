FID = 0
VCPU = 1
RAM_MB = 2
DISK_GB = 3

RESOURCE_TEMPLATES = (
    {
        # "micro"
        FID: 11,
        VCPU: 1,
        RAM_MB: 64,
        DISK_GB: 64,
    },
    {
        # "small"
        FID: 12,
        VCPU: 1,
        RAM_MB: 1024,
        DISK_GB: 256,
    },
    {
        # "medium"
        FID: 13,
        VCPU: 2,
        RAM_MB: 4096,
        DISK_GB: 1024,
    },
    {
        # "large"
        FID: 14,
        VCPU: 4,
        RAM_MB: 8192,
        DISK_GB: 2048,
    },
    {
        # "xlarge"
        FID: 15,
        VCPU: 8,
        RAM_MB: 16384,
        DISK_GB: 4096,
    },
    {
        # "xxlarge"
        FID: 16,
        VCPU: 16,
        RAM_MB: 32768,
        DISK_GB: 8192,
    },
)
