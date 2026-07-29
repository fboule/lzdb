#!/usr/bin/env python3
"""
Demonstrates:
- cross-references (foreign keys)
- N-to-N relationships via lzdb_links
"""

import psycopg as pg
from lzdb import *

dbms = LZDB(
    pg.connect(
        dbname="test",
        host="localhost"
    ),
    traceon=True
)

# Base item
item1 = lzitem(
    param="2004",
    starttime="03-jan-2000:00:00:00",
    endtime="04-jan-2000:00:00:00"
)

# Cross-reference example
item2 = lzitem(refers=item1)

# Relationship example
sat = lzitem(name="sat1")
sat.link(item1)

dbms.commit()

print("Cross-reference item:", item2)
print("Linked items for sat1:")
for it in dbms.linkedItems(sat):
    print(it)
