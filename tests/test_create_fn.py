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

# Use cases:
#  * Create an item will create it automatically
#  * Duplicate records are allowed (PK not provided)
#  * A record can refer other ones (FK)
#  * A record can be updated
#  * A record is created providing the pkeys
#  * A record can be augmented (data fields only, no pk)
#
# Next use cases:
#  * A data field can be a FK too
#  * Better support for datetime fields

import psycopg as pg
from lzdb import *

LZDB.traceon = True

conn = pg.connect(dbname = 'test', host='127.0.0.1', user='postgres')
dbms = LZDB(conn)

# pkey is param, starttime, endtime
item1 = lzitem(dbms, param='2004', starttime='03-jan-2000:00:00:00', endtime='04-jan-2000:00:00:00')
item4 = lzitem(dbms, param='2004', starttime='04-jan-2000:00:00:00', endtime='05-jan-2000:00:00:00')

item1.collection().name("time of event")

dbms.commit()

# pkey is refers
item2 = lzitem(dbms, refers=item1)
item2['clusters'] = [1,2,3]
item2['freqmap']=[4,5,6]

# pkey is refers
item3 = lzitem(dbms, refers=item1)
item3['clusters']=[2,3,4]
item3['freqmap']=[5,6,7]

item2.collection().name("clusters and frequency map")

dbms.commit()

# pkey is refers1, refers2
item5 = lzitem(dbms, refers1=item1,refers2=item2)
# item5.set(k=v) and item5[k]=v are identical
item5.set(timefreq=[1,2,5])

item5.collection().name("time frequency")

dbms.commit()

# item5 is augmented after commit... alter table?
item5['freqmap'] = [2,3,5]

dbms.commit()
