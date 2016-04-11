FID = 0
VCPU = 1
RAM_MB = 2
DISK_GB = 3

RESOURCE_TEMPLATES = (
    {
        # "micro"
        FID: 151,
        VCPU: 1,
        RAM_MB: 64,
        DISK_GB: 64,
    },
    {
        # "small"
        FID: 152,
        VCPU: 1,
        RAM_MB: 1024,
        DISK_GB: 256,
    },
    {
        # "medium"
        FID: 153,
        VCPU: 2,
        RAM_MB: 4096,
        DISK_GB: 1024,
    },
    {
        # "large"
        FID: 154,
        VCPU: 4,
        RAM_MB: 8192,
        DISK_GB: 2048,
    },
    {
        # "xlarge"
        FID: 155,
        VCPU: 8,
        RAM_MB: 16384,
        DISK_GB: 4096,
    },
    {
        # "xxlarge"
        FID: 156,
        VCPU: 16,
        RAM_MB: 32768,
        DISK_GB: 8192,
    },
)
