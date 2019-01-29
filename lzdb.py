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

class LZDB(object):
    __db = None
    __collections = None

    class Collection(object):
        pkeys = None
        __items = None

        def __init__(self, dbitem):
            self.pkeys = dbitem.pkeys
            self.__items = []

        def insert(self, dbitem):
            replace = False
            if len(self.pkeys) > 0:
                searchfor = [ dbitem[x] for x in self.pkeys ]
                for i in range(len(self.__items)):
                    values = [ self.__items[i][x] for x in self.pkeys ]
                    if values == searchfor:
                        replace = True
                        self.__items[i] = dbitem
            if not replace: 
                self.__items.append(dbitem)

        def query(self, **pkeys):
            searchfor = [ pkeys[x] for x in self.pkeys ]
            for dbitem in self.__items:
                values = [ dbitem[x] for x in self.pkeys ]
                if values == searchfor: return dbitem
            return None

        def size(self):
            return len(self.__items)

        def commit(self, db):
            pkeys = ','.join(sorted(self.pkeys))
            kkeys = sorted([ x for x in self.__items[0].keys() if x not in self.pkeys ])
            keys = ",".join(kkeys)
            db.execute("insert into lzdb(pkeys,keys) values('%s','%s') on conflict(pkeys, keys) do nothing" % (pkeys, keys))

            db.execute("select id from lzdb where pkeys='%s' and keys='%s'" % (pkeys, keys))
            self.id = db.fetchone()[0]

            s = "create table if not exists lzdb_%i(id serial primary key" % self.id
            if len(self.pkeys) > 0:
                for k in self.pkeys:
                    dbitem = self.__items[0][k]
                    kk = '%s integer references lzdb_%i' % (k, dbitem.collection.id)
                    s="%s, %s" % (s, kk)
            k = ' varchar, '.join(kkeys) + ' varchar'
            s="%s, %s)" % (s, k)
            db.execute(s)

            for dbitem in self.__items:
                ss = (pkeys+','+keys).strip(',')
                s = "insert into lzdb_%i(%s) values(" % (self.id, ss)
                for k in self.pkeys:
                    s = s + str(dbitem[k].id) + ','
                for k in kkeys:
                    s = s + "'%s'," % dbitem[k]
                s = s.strip(',')+")"
                db.execute(s)
                db.execute('select lastval()')
                dbitem.id = db.fetchone()[0]

    def __init__(self, conn):
        self.__conn = conn
        self.__db = conn.cursor()
        self.__collections = []

    def insert(self, dbitem):
        current = None

        for collection in self.__collections:
            if dbitem.pkeys == collection.pkeys:
                current = collection
                break

        if current is None:
            current = LZDB.Collection(dbitem)
            self.__collections.append(current)

        current.insert(dbitem)
        dbitem.collection = current

    def size(self, pkeys):
        current = None

        for collection in self.__collections:
            if pkeys == collection.pkeys:
                return collection.size()

        return None
        
    def query(self, **pkeys):
        current = None

        for collection in self.__collections:
            if list(pkeys.keys()) == collection.pkeys:
                current = collection
                break

        if current is None:
            return None

        return current.query(**pkeys)

    def commit(self):
        self.__db.execute('create table if not exists lzdb(id serial primary key, pkeys varchar, keys varchar, unique(pkeys, keys))')
        for collection in self.__collections:
            collection.commit(self.__db)
        self.__conn.commit()

class lzdbItem(dict):
    pkeys = None
    collection = None
    id = None

    def __init__(self, **refs):
        self.pkeys = list(refs.keys())
        for k, ref in refs.items():
            self[k] = ref
        
