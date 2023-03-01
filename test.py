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
# * Create an item will create it automatically
# * Duplicate records are allowed (PK not provided)
# * A record can refer other ones (FK)
# * A record can be updated
# * Thin provisioning: item with fields a,b,c matches table with fields a,b,c,d,e?
# * Smart provisioning: item creation can be declared "like" another (no thin provisioning, but prefill item with None values)
# * A record can be augmented before commit? (TBD) move item around
# * A record can be augmented after commit? (TBD) alter table? move item to another table? what about FKs? too heavy to implement?

import psycopg2 as pg
from lzdb import *

LZDB.traceon = True

conn = pg.connect(database = 'test', host='dbms')
dbms = LZDB(conn)

# pkey is param, starttime, endtime
item1 = dbms.newItem(param='2004', starttime='03-jan-2000:00:00:00', endtime='04-jan-2000:00:00:00')
item4 = dbms.newItem(param='2004', starttime='04-jan-2000:00:00:00', endtime='05-jan-2000:00:00:00')

# pkey is refers
item2 = dbms.newItem(refers=item1)
item2['clusters'] = [1,2,3]
item2['freqmap']=[4,5,6]

# pkey is refers
item3 = dbms.newItem(refers=item1)
item3['clusters']=[2,3,4]
item3['freqmap']=[5,6,7]

dbms.commit()

# pkey is refers1, refers2
item5 = dbms.newItem(refers1=item1,refers2=item2)
item5.assign(timefreq=[1,2,3])

dbms.commit()

# item5 is augmented after commit... alter table?
item5['freqmap'] = [2,3,4]

dbms.commit()
