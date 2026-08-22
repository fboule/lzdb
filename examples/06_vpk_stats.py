from lzdb import *
import psycopg as pg
from lzdb.stats.stats_vpk import reassess_vpk

dbms = LZDB(    pg.connect(        dbname="test",        host="localhost"    ),    traceon=False)
dbms.expose()

for collection in dbms.collections():
    items = dbms.items(collection=collection)
    if items:
        suggestion = reassess_vpk(items,candidate_fields=items[0].keys(),time_field="starttime")
        pp(collection.id + " " + collection.name())
        pp(suggestion)
