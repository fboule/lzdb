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

from lzdb import *

class Dummy(object):
    __counter = 0

    def cursor(self):
        return self

    def execute(self, s):
        print(s)

    def commit(self):
        print("Dummy commit")

    def fetchone(self):
        self.__counter = self.__counter + 1
        return [self.__counter]


db = Dummy()
dbms = LZDB(db)

item1 = lzdbItem()
item1['param'] = '2004'
item1['starttime'] = '01-jan-2000:00:00:00'
item1['endtime'] = '02-jan-2000:00:00:00'

item4 = lzdbItem()
item4['param'] = '2004'
item4['starttime'] = '02-jan-2000:00:00:00'
item4['endtime'] = '03-jan-2000:00:00:00'

item2 = lzdbItem(pattern = item1)
item2['clusters'] = [1,2,3]
item2['freqmap']=[4,5,6]

item3 = lzdbItem(pattern=item4)
item3['clusters']=[2,3,4]
item3['freqmap']=[5,6,7]

dbms.insert(item1)
dbms.insert(item4)
dbms.insert(item2)
dbms.insert(item3)

dbms.commit()

