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
import glob
import pandas as pd
import pprint

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
                if isinstance(v, LZDB.lzdbItem): self.__fkeys[k] = v.collection()
            if collection is None:
                self.__collection = dbms.collections(self.__ukeys, self.__fkeys)
            else:
                self.__collection = collection

        def id(self, id = None):
            if id is not None: self.__id = id
            return self.__id

        def set(self, **kwargs):
            for k, v in kwargs.items():
                self[k] = v

        def foreignKeys(self):
            return self.__fkeys

        def uniqueKeys(self):
            return self.__ukeys

        def uniqueDict(self):
            v = {}
            for k in self.__ukeys:
                v[k] = self[k]
            return v

        def collection(self):
            return self.__collection

    class Collection(object):
        __id = None
        __ukeys = None
        __fkeys = None
        __fields = None
        __dbms = None
        __tname = ''

        def __init__(self, dbms, ukeys=None, fkeys={}, dbitem=None, tname=''):
            self.__dbms = dbms
            self.__tname = tname
            if ukeys is not None:
                self.__fkeys = fkeys
                self.__fields = []
            if dbitem is not None:
                ukeys = dbitem.uniqueKeys()
                self.__fkeys = dbitem.foreignKeys()
            if ukeys is not None:
                self.__ukeys = sorted(ukeys)
                self.__fields.extend(ukeys)
            for field in self.__fkeys:
                if field not in self.__fields: self.__fields.append(field)

        def id(self):
            return self.__id

        def name(self, tname = None):
            if tname is not None:
                self.__tname = tname
            return self.__tname

        def uniqueKeys(self):
            return self.__ukeys

        def read(self, db, id):
            self.__id = id
            self.read_fkeys(db, id)
            rows = db.execute("select * from %s" % id)
            self.__fields = [desc[0] for desc in db.description]
            if LZDB.traceon:
                tname = ""
                if self.__tname != '': tname = f" as '{self.__tname}'"
                if len(self.__fkeys) == 0:
                    print('Found %i rows in %s(%s)%s' % (rows.rowcount, id, ','.join(self.__ukeys), tname))
                else:
                    print('Found %i rows in %s(%s)%s with references:' % (rows.rowcount, id, ','.join(self.__ukeys), tname))
                    for name, collection in self.__fkeys.items():
                        print(f'  {name} to {collection.id()}')
            for row in rows:
                pkitems = dict(zip(self.__fields, row))
                items = {}
                for kk in self.__fields:
                    if kk in self.__fkeys:
                        items[kk] = self.__dbms.items(collection = self.__fkeys[kk], id = pkitems[kk])
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
                self.__fkeys[field] = self.__dbms.collections(id = collid)

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
                f"insert into lzdb(ukeys, tname) values('{ukeys}','{self.__tname}') on conflict(ukeys) do update set ukeys = EXCLUDED.ukeys, tname = '{self.__tname}' returning id")
            self.__id= f"lzdb__{res.fetchone()[0]}"

            s = "create table if not exists %s(id serial primary key" % self.__id
            if len(self.__fkeys) > 0:
                for k, collection in self.__fkeys.items():
                    kk = '%s integer references %s' % (k, collection.id())
                    s = "%s, %s" % (s, kk)
            k = ', '.join([f'{x} varchar' for x in fields if x not in self.__fkeys])
            if k != '': s = f'{s}, {k}'
            if len(ukeys) > 0: s = s + ", unique(%s)" % ukeys
            s = s + ");"
            db.execute(s)
            for field in self.__ukeys + list(self.__fkeys.keys()):
                if field not in self.__fields: self.__fields.append(field)

    def __init__(self, conn, traceon = False):
        import inspect

        self.__conn = conn
        self.__db = conn.cursor()
        self.__collections = []
        self.__items = []
        LZDB.traceon = traceon

        db = conn.cursor()
        db.execute(
            "select exists(select 1 from information_schema.tables where table_schema='public' and table_name='lzdb')")
        if not db.fetchone()[0]: return

        db.execute("select id, ukeys, tname from lzdb")
        tables = db.fetchall()
        if LZDB.traceon: print('LZDB tables found:', len(tables))
        for table in tables:
            ukeys = None
            if len(table[1]) > 0: ukeys = table[1].split(',')
            id = f'lzdb__{table[0]}'
            tname = table[2]
            collection = LZDB.Collection(self, ukeys=ukeys, tname=tname)
            collection.read_fkeys(db, id)
            self.__collections.append(collection)
            collection.read(db, id)

        self.register(stack = inspect.stack()[1])

    def register(self, stack = None):
        import inspect
        if stack is None:
            stack = inspect.stack()[1]
        caller_globals = stack.frame.f_globals
        ptrs = {
            'lzitem': 'newItem',
            'lzitems': 'items',
            'lzc': 'collections',
            'lzcnames': 'collectionsNames',
        }
        for k, v in ptrs.items():
            caller_globals[k] = getattr(self, v)
        caller_globals['dd'] = lzdict()
        caller_globals['pp'] = pprint.PrettyPrinter().pprint

    def commit(self):
        self.__db.execute('create table if not exists lzdb(id serial primary key, ukeys varchar, tname varchar, unique(ukeys))')
        for collection in self.__collections:
            collection.createTable(self.__db)
        for dbitem in self.__items:
            dbitem.collection().createNewFields(self.__db, dbitem)
            fields = sorted(dbitem.keys())
            ukeys = dbitem.uniqueKeys()
            datafields = sorted([x for x in fields if x not in ukeys])
            s = "insert into %s(%s) values(" % (dbitem.collection().id(), ','.join(fields))
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
            if refs == item.uniqueDict():
                dbitem = item
                dbitem.set(**refs)
                break
        if dbitem is None:
            dbitem = self.lzdbItem(self, collection = collection, **refs)
            self.__items.append(dbitem)
        dbitem.id(id)
        return dbitem

    def collectionsNames(self):
        return [ collection.name() for collection in self.__collections ]

    def collections(self, ukeys = None, fkeys = None, id = None, name = None):
        if name is not None:
            for collection in self.__collections:
                if collection.name() == name:
                    return collection
            return None
        if id is not None:
            for collection in self.__collections:
                if collection.id() == id:
                    return collection
            return None
        if ukeys is None:
            return self.__collections
        ukeys = sorted(ukeys)
        for collection in self.__collections:
            if collection.uniqueKeys() == ukeys:
                return collection
        collection = LZDB.Collection(self, ukeys=ukeys, fkeys=fkeys)
        self.__collections.append(collection)
        return collection

    def items(self, collection = None, **refs):
        if len(refs) == 0 and collection is None:
            return self.__items
        items = []
        if collection is not None and 'id' in refs:
            for item in self.__items:
                if item.id() == refs['id'] and item.collection() == collection:
                    return item
            return None
        elif collection is not None:
            for item in self.__items:
                if item.collection() == collection:
                    items.append(item)
        else:
            for item in self.__items:
                if refs.items() <= item.items():
                    items.append(item)
        return items

class lzdict(dict):
    __loader = None

    class parquet(object):
        def get(self, name, folder = "data"):
            filelist = glob.glob("%s/*%s*" % (folder, name))
            if len(filelist) != 1:
                return None
            filepath = filelist[0]
            filename = filepath.split('_')[0].split('/')[1]
            if LZDB.traceon:
                print("Parquet::Get %s" % filename)
            return pd.read_parquet(filepath)

    def __init__(self, loader = None):
        self.__loader = loader
        if loader is None:
            self.__loader = lzdict.parquet()

    def __getitem__(self, key):
        if not super().__contains__(key):
            self[key] = self.__loader.get(key)
        return super().__getitem__(key)

