################################################################################
#                                                                               
#  Copyright (C) 2019 Fabien Bouleau
#
#  This file is part of lzdb.
#
# lzdb is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# lzdb is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with lzdb. If not, see <http://www.gnu.org/licenses/>.
#
################################################################################

import psycopg as pg
from lzdb import *
import pprint
pp = pprint.PrettyPrinter()

LZDB.traceon = True

conn = pg.connect(dbname = 'test', host='127.0.0.1', user='postgres')
dbms = LZDB(conn)

print("\nLooking up param='2004':")
items = dbms.items(param='2004')
pp.pprint(items)

print("\nLooking up collection 'time frequency':")
collection = dbms.findCollectionByName('time frequency')
items = dbms.items(collection)
pp.pprint(items)

print("\nLooking up collection 'time of event':")
collection = dbms.findCollectionByName('time of event')
items = dbms.items(collection)
pp.pprint(items)
