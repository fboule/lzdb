#!/usr/bin/env python3
"""
Basic usage of lzdb:
- connecting to PostgreSQL
- creating items
- committing changes
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

# Create a simple item
item = dbms.newItem(
    param="2004",
    starttime="03-jan-2000:00:00:00",
    endtime="04-jan-2000:00:00:00"
)

print("Created item:", item)

dbms.commit()
print("Committed.")
