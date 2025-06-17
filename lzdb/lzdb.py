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
    __items = None
    traceon = False

    class lzdbItem(dict):
        __ukeys = None
        __fkeys = None
        __collection = None
        __id = None

        def __init__(self, dbms, id=None, collection=None, **refs):
            self.__ukeys = sorted(refs.keys())
            self.__fkeys = {}
            self.__id = id
            for k, v in refs.items():
                self[k] = v
                if isinstance(v, LZDB.lzdbItem): self.__fkeys[k] = v.getCollection()
            if collection is None:
                self.__collection = dbms.fetchCollection(self.__ukeys, self.__fkeys)
            else:
                self.__collection = collection

        def id(self, id = None):
            if id is not None: self.__id = id
            return self.__id

        def set(self, **kwargs):
            for k, v in kwargs.items():
                self[k] = v

        def getForeignKeys(self):
            return self.__fkeys

        def getUniqueKeys(self):
            return self.__ukeys

        def getUniqueDict(self):
            v = {}
            for k in self.__ukeys:
                v[k] = self[k]
            return v

        def getCollection(self):
            return self.__collection

    class Collection(object):
        __id = None
        __ukeys = None
        __fkeys = None
        __fields = None
        __dbms = None

        def __init__(self, dbms, ukeys=None, fkeys={}, dbitem=None):
            self.__dbms = dbms
            if ukeys is not None:
                self.__fkeys = fkeys
                self.__fields = []
            if dbitem is not None:
                ukeys = dbitem.getUniqueKeys()
                self.__fkeys = dbitem.getForeignKeys()
            if ukeys is not None:
                self.__ukeys = sorted(ukeys)
                self.__fields.extend(ukeys)
            for field in self.__fkeys:
                if field not in self.__fields: self.__fields.append(field)

        def getId(self):
            return self.__id

        def getUniqueKeys(self):
            return self.__ukeys

        def read(self, db, id):
            self.__id = id
            self.read_fkeys(db, id)
            rows = db.execute("select * from %s" % id)
            self.__fields = [desc[0] for desc in db.description]
            if LZDB.traceon:
                if len(self.__fkeys) == 0:
                    print('Found %i rows in %s(%s)' % (rows.rowcount, id, ','.join(self.__ukeys)))
                else:
                    print('Found %i rows in %s(%s) with references:' % (rows.rowcount, id, ','.join(self.__ukeys)))
                    for name, collection in self.__fkeys.items():
                        print(f'  {name} to {collection.getId()}')
            for row in rows:
                pkitems = dict(zip(self.__fields, row))
                items = {}
                for kk in self.__fields:
                    if kk in self.__fkeys:
                        items[kk] = self.__dbms.findItem(self.__fkeys[kk], pkitems[kk])
                    else:
                        try:
                            items[kk] = datetime.datetime.strptime(pkitems[kk], "%Y-%m-%d %H:%M:%S")
                        except:
                            items[kk] = pkitems[kk]
                obj = {}
                for field in self.__ukeys:
                    obj[field] = items[field]
                dbitem = self.__dbms.newItem(collection=self, **obj)
                dbitem.id(items['id'])
                for field in items:
                    if field not in self.__ukeys:
                        dbitem[field] = items[field]

        def read_fkeys(self, db, id):
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
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='%s';""" % id
            db.execute(s)
            items = db.fetchall()
            self.__fkeys = {}
            for field, collid in dict(items).items():
                self.__fkeys[field] = self.__dbms.findCollectionByName(collid)

        def createNewFields(self, db, dbitem):
            newFields = []
            for field in dbitem.keys():
                if field not in self.__fields:
                    newFields.append(field)
            if len(newFields) == 0: return
            s = f'alter table {self.__id} ' + ', '.join([ f'add column {x} varchar' for x in newFields])
            db.execute(s)
            self.__fields.extend(newFields)

        def createTable(self, db):
            ukeys = ','.join(self.__ukeys)
            fields = sorted([x for x in self.__fields if x != 'id'])
            res = db.execute(
                f"insert into lzdb(ukeys) values('{ukeys}') on conflict(ukeys) do update set ukeys = EXCLUDED.ukeys returning id")
            self.__id= f"lzdb__{res.fetchone()[0]}"

            s = "create table if not exists %s(id serial primary key" % self.__id
            if len(self.__fkeys) > 0:
                for k, collection in self.__fkeys.items():
                    kk = '%s integer references %s' % (k, collection.getId())
                    s = "%s, %s" % (s, kk)
            k = ', '.join([f'{x} varchar' for x in fields if x not in self.__fkeys])
            if k != '': s = f'{s}, {k}'
            if len(ukeys) > 0: s = s + ", unique(%s)" % ukeys
            s = s + ");"
            db.execute(s)
            for field in self.__ukeys + list(self.__fkeys.keys()):
                if field not in self.__fields: self.__fields.append(field)

    def __init__(self, conn):
        self.__conn = conn
        self.__db = conn.cursor()
        self.__collections = []
        self.__items = []

        db = conn.cursor()
        db.execute(
            "select exists(select 1 from information_schema.tables where table_schema='public' and table_name='lzdb')")
        if not db.fetchone()[0]: return

        db.execute("select id, ukeys from lzdb")
        tables = db.fetchall()
        if LZDB.traceon: print('LZDB tables found:', len(tables))
        for table in tables:
            ukeys = None
            if len(table[1]) > 0: ukeys = table[1].split(',')
            id = f'lzdb__{table[0]}'
            collection = LZDB.Collection(self, ukeys=ukeys)
            collection.read_fkeys(db, id)
            self.__collections.append(collection)
            collection.read(db, id)

    def commit(self):
        self.__db.execute('create table if not exists lzdb(id serial primary key, ukeys varchar, unique(ukeys))')
        for collection in self.__collections:
            collection.createTable(self.__db)
        for dbitem in self.__items:
            dbitem.getCollection().createNewFields(self.__db, dbitem)
            fields = sorted(dbitem.keys())
            ukeys = dbitem.getUniqueKeys()
            datafields = sorted([x for x in fields if x not in ukeys])
            s = "insert into %s(%s) values(" % (dbitem.getCollection().getId(), ','.join(fields))
            items = []
            for field in fields:
                if isinstance(dbitem[field], LZDB.lzdbItem):
                    items.append(str(dbitem[field].id()))
                else:
                    items.append((dbitem[field]))
            s = s + ', '.join([f"'{x}'" for x in items]) + ")"
            if len(datafields) > 0 and len(ukeys) > 0:
                s = s + " on conflict(%s) do update set " % ','.join(ukeys)
                kk = []
                for k in datafields:
                    kk.append("%s = EXCLUDED.%s" % (k, k))
                s = s + ', '.join(kk)
            else:
                s = s + f" on conflict({','.join(ukeys)}) do nothing"
            s = s + " returning id;"
            self.__db.execute(s)
            res = self.__db.fetchone()
            if res is not None:
                dbitem.id(res[0])
        self.__conn.commit()

    def newItem(self, collection=None, id=None, **refs):
        dbitem = None
        for item in self.__items:
            if refs == item.getUniqueDict():
                dbitem = item
                dbitem.set(**refs)
                break
        if dbitem is None:
            dbitem = self.lzdbItem(self, collection = collection, **refs)
            self.__items.append(dbitem)
        dbitem.id(id)
        return dbitem

    def fetchCollection(self, ukeys, fkeys):
        ukeys = sorted(ukeys)
        for collection in self.__collections:
            if collection.getUniqueKeys() == ukeys:
                return collection
        collection = LZDB.Collection(self, ukeys=ukeys, fkeys=fkeys)
        self.__collections.append(collection)
        return collection

    def findItem(self, collection, v):
        for item in self.__items:
            if item.id() == v and item.getCollection() == collection:
                return item
        return None

    def findCollectionByName(self, collid):
        for collection in self.__collections:
            if collection.getId() == collid:
                return collection
        return None
    
    def getItems(self, **refs):
        if len(refs) == 0:
            return self.__items
        items = []
        for item in self.__items:
            if refs.items() <= item.items():
                items.append(item)
        return items

