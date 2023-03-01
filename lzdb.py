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

import datetime

class LZDB(object):
    __db = None
    __collections = None
    traceon = False
    staged = []

    class lzdbItem(dict):
        pkeys = None
        collection = None
        id = None

        def __init__(self, **refs):
            self.pkeys = list(refs.keys())
            for k, ref in refs.items():
                self[k] = ref


    class Collection(object):
        id = None
        pkeys = []
        keys = None
        __items = None
        __dbms = None
        __fkeys = None

        def __init__(self, dbms, dbitem = None):
            self.__dbms = dbms
            if dbitem is not None: 
                self.pkeys = dbitem.pkeys
                self.keys = [ x for x in dbitem.keys() if x not in dbitem.pkeys ]
            self.__items = []

        def read(self, db, id, pkeys, keys):
            self.id = id
            k = []
            if len(pkeys) > 0: 
                k = pkeys.split(',')
                self.read_fkeys(db, id)
            keys = keys.split(',')
            k.extend(keys)
            db.execute("select id,%s from lzdb_%s" % (','.join(k), id))
            rows = db.fetchall()
            if LZDB.traceon: 
                if len(pkeys) == 0: 
                    print('Found %i rows in lzdb_%s(%s)' % (len(rows),id,','.join(k)))
                else: 
                    print('Found %i rows in lzdb_%s(%s) with references:' % (len(rows),id,','.join(k)))
                    for frow in self.__fkeys:
                        print('  %(name)s to %(table)s' % frow)
            for row in rows:
                items = dict(zip(['id'] + k, row))
                dbitem = self.__dbms.newItem(staging = False)
                dbitem.id = int(items['id'])
                for kk in k: 
                    if kk in pkeys: 
                        dbitem[kk] = self.lookup(kk, items[kk])
                    else:
                        dbitem[kk] = None
                        try:
                            dbitem[kk] = eval(items[kk])
                        except:
                            pass
                        if dbitem[kk] is None:
                            try:
                                dbitem[kk] = datetime.datetime.strptime(items[kk], "%Y-%m-%d %H:%M:%S")
                            except:
                                dbitem[kk] = items[kk]
                dbitem.collection = self
                self.__items.append(dbitem)

        def lookup(self, k, v):
            collid = None
            for row in self.__fkeys:
                if row['name'] == k:
                    collid = row['table'].strip('lzdb_')
            if collid is None: return None
            coll = self.__dbms.querycollbyid(collid)
            return coll.querybyid(v)

        def read_fkeys(self, db, id):
            if self.__fkeys is None:
                s = """SELECT 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name 
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='lzdb_%s';""" % id
                db.execute(s)
                items = db.fetchall()
                self.__fkeys = []
                for item in items:
                    self.__fkeys.append(dict(zip(['name', 'table'], item)))

        def insert(self, dbitem):
            replace = False
            if self.keys is None:
                self.keys = [ x for x in dbitem.keys() if x not in dbitem.pkeys ]
            if len(self.pkeys) > 0:
                searchfor = [ dbitem[x] for x in self.pkeys ]
                for i in range(len(self.__items)):
                    values = [ self.__items[i][x] for x in self.pkeys ]
                    if values == searchfor:
                        replace = True
                        self.__items[i] = dbitem
            if not replace: 
                dbitem.collection = self
                self.__items.append(dbitem)

        def query(self, **pkeys):
            searchfor = sorted([ x for x in pkeys.keys() if pkeys[x] is not None])
            v1 = [ pkeys[x] for x in searchfor if pkeys[x] is not None ]
            for dbitem in self.__items:
                values = [ dbitem[x] for x in searchfor ]
                if values == v1: return dbitem
            return None

        def querybyid(self, id):
            for dbitem in self.__items:
                if dbitem.id == id: return dbitem
            return None

        def size(self):
            return len(self.__items)

        def commit(self, db):
            pkeys = ','.join(sorted(self.pkeys))
            akeys = sorted(self.__items[0].keys())
            kkeys = sorted([ x for x in akeys if x not in self.pkeys ])
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
            s="%s, %s" % (s, k)
            if len(pkeys) > 0:
                s = s + ", unique(%s))" % pkeys
            else:
                s = s + ", unique(%s))" % ','.join(akeys)
            db.execute(s)

            for dbitem in self.__items:
                ss = (pkeys+','+keys).strip(',')
                s = "begin; insert into lzdb_%i(%s) values(" % (self.id, ss)
                for k in self.pkeys:
                    s = s + str(dbitem[k].id) + ','
                for k in kkeys:
                    s = s + "'%s'," % dbitem[k]
                s = s.strip(',')+") " 
                if len(self.pkeys) > 0: 
                    s = s + "on conflict(%s) do update set " % ','.join(self.pkeys)
                    kk = []
                    for k in kkeys:
                        kk.append("%s = EXCLUDED.%s" % (k, k))
                    s = s + ', '.join(kk)
                else:
                    s = s + "on conflict(%s) do update set %s = EXCLUDED.%s" % (keys, akeys[0], akeys[0])
                s = s + " returning id;"
                db.execute(s)
                dbitem.id = db.fetchone()[0]

    def __init__(self, conn):
        self.__conn = conn
        self.__db = conn.cursor()
        self.__collections = {}

        db = conn.cursor()
        db.execute("select exists(select 1 from information_schema.tables where table_schema='public' and table_name='lzdb')")
        if not db.fetchone()[0]: return

        db.execute("select id, pkeys, keys from lzdb")
        items = db.fetchall()
        if LZDB.traceon: print('LZDB tables found:', len(items))
        for item in items:
            current = LZDB.Collection(self)
            if len(item[1]) > 0: current.pkeys = item[1].split(',')
            current.keys = item[2].split(',')
            current.read(db, *item)
            self.__collections[current.id] = current

    def stage(self, dbitem):
        self.staged.append(dbitem)

    def flush(self):
        for dbitem in self.staged:
            self.flushitem(dbitem)
        self.staged = []

    def flushitem(self, dbitem):
        current = None
        kkeys = sorted(dbitem.keys())

        for collection in self.__collections.values():
            ckeys = sorted(collection.keys)
            if collection.pkeys is not None: ckeys = sorted(ckeys + collection.pkeys)
            if ckeys == kkeys:
                if LZDB.traceon: 
                    kk = collection.pkeys or []
                    kk = sorted(kk + collection.keys)
                    print("Matching collection", kk)
                current = collection
                break

        if current is None:
            current = LZDB.Collection(self, dbitem)
            self.__collections[current.id] = current
            if LZDB.traceon: print("New collection", current.pkeys or '', current.keys)

        current.insert(dbitem)

    def size(self, pkeys):
        current = None

        for collection in self.__collections.values():
            if pkeys == collection.pkeys:
                return collection.size()

        return None
        
    def querycollbyid(self, id):
        return self.__collections[int(id)]

    def query(self, **pkeys):
        kk = sorted(pkeys.keys())
        for collection in self.__collections.values():
            if collection.pkeys is not None:
                if kk == sorted(collection.pkeys + collection.keys):
                    return collection.query(**pkeys)
            if kk == sorted(collection.keys):
                return collection.query(**pkeys)
        return None

    def commit(self):
        self.flush()
        self.__db.execute('create table if not exists lzdb(id serial primary key, pkeys varchar, keys varchar, unique(pkeys, keys))')
        for collection in self.__collections.values():
            collection.commit(self.__db)
        self.__conn.commit()

    def newItem(self, staging = True, **refs):
        dbitem = self.lzdbItem(**refs)
        if staging:
            self.stage(dbitem)
        else:
            self.flushitem(dbitem)
        return dbitem

