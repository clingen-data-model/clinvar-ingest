"""
vrsify's option values and defaults, shared by the command line, runner.vrsify and the
Cloud Batch job definition (batch.BatchSettings).

This module imports nothing, so the command line can read it without loading the
`vrsify` dependencies.
"""

# How a RefgetStore serves reads; see runner.SequenceSource
REFGET_READS = ("whole", "ranges")
REFGET_READS_DEFAULT = "whole"
# Sequence files fetched at a time when prefetching a remote store
PREFETCH_THREADS = 64
# Records per worker task
BATCH_SIZE = 50
# Seconds before a record becomes an error
RECORD_TIMEOUT_SECONDS = 300
# The Batch job's VM, and its boot disk, which holds the image and the prefetched sequences
BATCH_MACHINE_TYPE = "c4-highmem-4"
BATCH_BOOT_DISK_SIZE_GB = 50
