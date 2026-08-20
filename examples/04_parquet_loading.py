#!/usr/bin/env python3
"""
Demonstrates parquet loading via lzdict:
- automatic file discovery
- caching
"""

from lzdb import lzdict
import pprint

pp = pprint.PrettyPrinter().pprint

ld = lzdict()

# Load any file matching *PQTFILE* in data/
data = ld["PQTFILE"]

print("Loaded parquet data:")
pp(data)

print("Keys in dictionary:", ld.keys())
