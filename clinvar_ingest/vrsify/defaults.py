"""
vrsify's option values and defaults, shared by the command line and runner.vrsify.

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
