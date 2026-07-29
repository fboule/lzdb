#!/usr/bin/env python3
"""
Demonstrates schema emergence:
- virtual primary key inference
- automatic table creation
- lazy field addition
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

# First item defines the virtual primary key
item1 = dbms.newItem(
    param="2004",
    starttime="03-jan-2000:00:00:00",
    endtime="04-jan-2000:00:00:00"
)

# Second item with same virtual PK goes into same table
item2 = dbms.newItem(
    param="2004",
    starttime="04-jan-2000:00:00:00",
    endtime="05-jan-2000:00:00:00"
)

# Add new fields lazily
item2["clusters"] = [1, 2, 3]
item2["freqmap"] = [4, 5, 6]

dbms.commit()

print("Items in collection:")
for it in dbms.items(param="2004"):
    print(it)
