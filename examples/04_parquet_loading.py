#!/usr/bin/env python3
"""
Demonstrates parquet loading via lzdict:
- automatic file discovery
- caching
"""

from lzdb import lzdict
import pprint

pp = pprint.PrettyPrinter().pprint

dd = lzdict()

# Load any file matching *PQTFILE* in data/
data = dd["PQTFILE"]

print("Loaded parquet data:")
pp(data)

print("Keys in dictionary:", dd.keys())
