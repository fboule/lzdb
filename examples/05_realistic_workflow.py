#!/usr/bin/env python3
"""
A realistic workflow combining:
- item creation
- schema emergence
- cross-references
- relationships
- querying
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
dbms.expose()

# Create several measurement items
measurements = [
    lzitem(param="2004", starttime="03-jan-2000:00:00:00", endtime="04-jan-2000:00:00:00"),
    lzitem(param="2005", starttime="03-jan-2000:00:00:00", endtime="04-jan-2000:00:00:00"),
    lzitem(param="2006", starttime="03-jan-2000:00:00:00", endtime="04-jan-2000:00:00:00")
]

# Satellite item
sat = lzitem(name="sat1")

# Link satellite to all measurements
sat.link(measurements)

# Add metadata lazily
sat["operator"] = "ESA"
sat["launch_year"] = 1998

dbms.commit()

print("Satellite metadata:")
pp(sat)

print("\nMeasurements linked to sat1:")
for it in dbms.linkedItems(sat):
    pp(it)
