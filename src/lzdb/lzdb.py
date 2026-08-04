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
import getpass

ACCOUNT_NAME = getpass.getuser()

class LZDB(object):
    __db = None
    __collections = None
    __items = None
    traceon = False

    REL_DIRECTED = 0
    REL_UNDIRECTED = 1

    class lzdbItem(dict):

        def __init__(self, collection, **kwargs):
            super().__init__()

            self.__collection = collection
            self.__id = None
            self.__loaded = False
            self.__dirty = True

            self.__links = []

            for k, v in kwargs.items():
                self[k] = v

        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            self.__dirty = True

        def markDirty(self):
            self.__dirty = True

        def clearDirty(self):
            self.__dirty = False

        def isDirty(self):
            return self.__dirty

        def foreignKeys(self):
            result = {}

            for field, value in self.items():
                if isinstance(value, LZDB.lzdbItem):
                    result[field] = value.collection()

            return result

        def markLoaded(self):
            self.__loaded = True

        def isLoaded(self):
            return self.__loaded

        def fields(self):
            return list(self.keys())

        def link(self, item, reltype=None):

            if reltype is None:
                reltype = LZDB.REL_DIRECTED

            if isinstance(item, list):

                for it in item:
                    self.link(it, reltype)

                return

            self.__links.append({
                "item": item,
                "reltype": reltype
            })

        def links(self):
            return self.__links

        def set(self, **kwargs):
            """
            Update fields of the item (original lzdb behavior).
            """
            for k, v in kwargs.items():
                self[k] = v

        def uniqueDict(self):
            """
            Return the virtual PK dictionary.
            This is used for deduplication and schema grouping.
            """
            return {k: self[k] for k in self.virtualKeys()}

        def collection(self):
            return self.__collection

        def id(self, value=None):
            if value is not None:
                self.__id = value
            return self.__id

        def virtualKeys(self):
            """
            Virtual PK = schema descriptor.
            These fields determine the table schema,
            NOT uniqueness constraints.
            """
            keys = []
            for k, v in self.items():
                if k == "id":
                    continue
                if k.startswith("refers"):
                    continue
                if isinstance(v, list):
                    continue
                keys.append(k)
            return sorted(keys)

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

            # Initialize fields
            self.__fields = []
            self.__fkeys = {}

            # CASE 1: Collection created from virtual PK signature
            if ukeys is not None:
                self.__ukeys = sorted(ukeys)
                self.__fields.extend(self.__ukeys)
                self.__fkeys = fkeys

            # CASE 2: Collection created from dbitem (loading from DB)
            if dbitem is not None:
                # IMPORTANT:
                # virtualKeys() define schema, NOT uniqueness
                self.__ukeys = dbitem.virtualKeys()
                self.__fields.extend(self.__ukeys)

            # Add FK fields to schema
            for field in self.__fkeys:
                if field not in self.__fields:
                    self.__fields.append(field)

        def id(self):
            return self.__id

        def name(self, tname=None):
            if tname is not None:
                self.__tname = tname
            return self.__tname

        def uniqueKeys(self):
            """
            ADAPTED:
            uniqueKeys = virtual PK fields (schema signature)
            NOT real PKs.
            """
            return self.__ukeys

        def read(self, db, id):
            self.__id = id
            self.read_fkeys(db, id)

            rows = db.execute("select * from %s" % id)
            self.__fields = [desc[0] for desc in db.description]

            if LZDB.traceon:
                tname = f" as '{self.__tname}'" if self.__tname else ""
                if len(self.__fkeys) == 0:
                    print(f"Found {rows.rowcount} rows in {id}({','.join(self.__ukeys)}){tname}")
                else:
                    print(f"Found {rows.rowcount} rows in {id}({','.join(self.__ukeys)}){tname} with references:")
                    for name, collection in self.__fkeys.items():
                        print(f"  {name} to {collection.id()}")

            for row in rows:
                pkitems = dict(zip(self.__fields, row))
                items = {}

                for kk in self.__fields:
                    if kk in self.__fkeys:
                        items[kk] = self.__dbms.items(collection=self.__fkeys[kk], id=pkitems[kk])
                    else:
                        try:
                            items[kk] = datetime.datetime.strptime(pkitems[kk], "%Y-%m-%d %H:%M:%S")
                        except:
                            items[kk] = pkitems[kk]

                obj = {}

                for field in (self.__ukeys or []):
                    obj[field] = items[field]

                for field in self.__fkeys:
                    obj[field] = items[field]

                dbitem = self.__dbms.newItem(collection=self, __loading = True, **obj)
                dbitem.id(items['id'])
                dbitem.markLoaded()

                for field in items:
                    if field not in (self.__ukeys or []):
                        dbitem[field] = items[field]
                        dbitem.markLoaded()
                        dbitem.clearDirty()

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
                coll = self.__dbms.collections(id=collid)
                self.__fkeys[field] = coll

        def createNewFields(self, db, dbitem):
            # Get existing columns
            db.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{self.__id}'
            """)
            existing = {row[0] for row in db.fetchall()}

            newFields = []

            for field in dbitem.fields():
                if field in existing:
                    continue

                value = dbitem.get(field)

                # TRUE foreign key: value is another lzdbItem
                if isinstance(value, LZDB.lzdbItem):
                    db.execute(
                        f"ALTER TABLE {self.__id} "
                        f"ADD COLUMN {field} INTEGER REFERENCES {value.collection().id()}"
                    )
                    continue

                # Normal field → VARCHAR
                newFields.append(field)

            # Add normal fields
            for field in newFields:
                db.execute(f"ALTER TABLE {self.__id} ADD COLUMN {field} VARCHAR")

            self.__fields.extend(newFields)

        def createTable(self, db):
            if self.__id is not None:
                return

            # Register this collection in the inventory table
            ukeys = ",".join(self.uniqueKeys() or [])
            tname = self.name() if hasattr(self, "name") else ""

            db.execute(
                """
                INSERT INTO lzdb(ukeys, tname)
                VALUES (%s, %s)
                ON CONFLICT (ukeys)
                DO UPDATE SET tname = EXCLUDED.tname
                RETURNING id
                """,
                (ukeys, tname),
            )
            res = db.fetchone()
            self.__id = f"lzdb__{res[0]}"

            # Build CREATE TABLE statement for the actual collection table
            s = f"CREATE TABLE IF NOT EXISTS {self.__id}(id SERIAL PRIMARY KEY"

            # Foreign keys
            for k, collection in self.__fkeys.items():
                fk = f"{k} INTEGER REFERENCES {collection.id()}"
                s += f", {fk}"

            fields = self.uniqueKeys() or []
            data_fields = [f"{x} VARCHAR" for x in fields if x not in self.__fkeys]
            if data_fields:
                s += ", " + ", ".join(data_fields)

            # Close CREATE TABLE
            s += ");"

            db.execute(s)

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
        if not db.fetchone()[0]:
            db.execute("""
                CREATE TABLE IF NOT EXISTS lzdb (
                    id SERIAL PRIMARY KEY,
                    ukeys TEXT UNIQUE,
                    tname TEXT
                );
            """)


        db.execute("select id, ukeys, tname from lzdb")
        tables = db.fetchall()

        # Pass 1: create all collections
        for table in tables:

            ukeys = table[1].split(',') if table[1] else []

            collection = LZDB.Collection(
                self,
                ukeys=ukeys,
                tname=table[2]
            )

            collection._Collection__id = f"lzdb__{table[0]}"

            self.__collections.append(collection)

        # Pass 2: resolve FKs and load rows
        for collection in self.__collections:

            collection.read_fkeys(
                db,
                collection.id()
            )

        for collection in self.__collections:

            collection.read(
                db,
                collection.id()
            )

    @property
    def conn(self):
        return self.__conn

    def register(self):
        import inspect
        import pprint

        g = inspect.currentframe().f_back.f_globals

        g.update({
            'lzitem': self.newItem,
            'lzitems': self.items,
            'lzc': self.collections,
            'lzcnames': self.collectionsNames,
            'dd': lzdict(),
            'pp': pprint.PrettyPrinter().pprint,
         })

    def ensure(self, **refs):
        matches = self.items(**refs)

        if matches:
            return min(
                matches,
                key=lambda item: item.id()
            )

        return self.newItem(**refs)

    def commit(self):
        self.__createSystemTables()
        self.__createCollections()
        self.__saveItems()
        self.__saveLinks()

        self.__conn.commit()

    def __createSystemTables(self):
        self.__db.execute("""
            create table if not exists lzdb(
                id serial primary key,
                ukeys varchar,
                tname varchar,
                unique(ukeys)
            )
        """)

        self.__db.execute("""
            create table if not exists lzdb_links(
                src_collection integer not null,
                src_id integer not null,

                dst_collection integer not null,
                dst_id integer not null,

                reltype smallint not null default 0,

                unique(
                    src_collection,
                    src_id,
                    dst_collection,
                    dst_id,
                    reltype
                )
            )
        """)

    def __createCollections(self):
        for collection in self.__collections:
            collection.createTable(self.__db)

    def __saveItem(self, dbitem):
        coll = dbitem.collection()

        # Ensure schema is up to date
        coll.createNewFields(self.__db, dbitem)

        fields = []
        values = []

        for field in sorted(dbitem.keys()):

            if field == "id":
                continue

            value = dbitem[field]

            if isinstance(value, LZDB.lzdbItem):
                value = value.id()

            fields.append(field)
            values.append(value)

        # ------------------------------------------------------------------
        # UPDATE existing row
        # ------------------------------------------------------------------
        if dbitem.isLoaded():

            if len(fields) > 0:

                assignments = [
                    f"{field}=%s"
                    for field in fields
                ]

                sql = (
                    f"UPDATE {coll.id()} "
                    f"SET {', '.join(assignments)} "
                    f"WHERE id=%s"
                )

                params = values + [dbitem.id()]

                self.__db.execute(sql, params)

            dbitem.clearDirty()

            return

        # ------------------------------------------------------------------
        # INSERT new row
        # ------------------------------------------------------------------
        if len(fields) == 0:

            sql = (
                f"INSERT INTO {coll.id()} "
                f"DEFAULT VALUES "
                f"RETURNING id"
            )

            self.__db.execute(sql)

        else:

            placeholders = ", ".join(["%s"] * len(values))

            sql = (
                f"INSERT INTO {coll.id()} "
                f"({','.join(fields)}) "
                f"VALUES ({placeholders}) "
                f"RETURNING id"
            )

            self.__db.execute(sql, values)

        res = self.__db.fetchone()

        if res is not None:
            dbitem.id(res[0])

        dbitem.markLoaded()
        dbitem.clearDirty()

    def __saveItems(self):
        for dbitem in self.__items:

            if not dbitem.isDirty():
                continue

            self.__saveItem(dbitem)

    def __insertLink(self, src, dst, reltype):
        self.__db.execute(
            """
            insert into lzdb_links(
                src_collection,
                src_id,
                dst_collection,
                dst_id,
                reltype
            )
            values(
                %s,%s,%s,%s,%s
            )
            on conflict do nothing
            """,
            (
                src.collection().id().split('__')[1],
                src.id(),
                dst.collection().id().split('__')[1],
                dst.id(),
                reltype
            )
        )

    def __saveLinks(self):
        for dbitem in self.__items:

            for link in dbitem.links():

                target = link['item']
                reltype = link['reltype']

                if dbitem.id() is None:
                    continue

                if target.id() is None:
                    continue

                self.__insertLink(
                    dbitem,
                    target,
                    reltype
                )

                if reltype == LZDB.REL_UNDIRECTED:

                    self.__insertLink(
                        target,
                        dbitem,
                        reltype
                    )

    def newItem(self, collection=None, id=None, __loading=False, **refs):
        # If no collection provided, derive one from virtual PK
        if collection is None:
            temp = self.lzdbItem(None, **refs)

            ukeys = temp.virtualKeys()
            fkeys = temp.foreignKeys()

            # Try existing collection
            for coll in self.__collections:
                if coll.uniqueKeys() == ukeys:
                    collection = coll
                    break


            # Otherwise create new collection
            if collection is None:
                collection = LZDB.Collection(self, ukeys=ukeys, fkeys=fkeys, dbitem=None, tname='')
                self.__collections.append(collection)

        # Create item bound to collection
        dbitem = self.lzdbItem(collection, **refs)
        self.__items.append(dbitem)

        if id is not None:
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

    def linkedItems(self, item, reltype=None):
        sql = """
            select
                dst_collection,
                dst_id
            from
                lzdb_links
            where
                src_collection=%s
            and
                src_id=%s
        """

        params = [
            int(item.collection().id().split('__')[1]),
            item.id()
        ]

        if reltype is not None:
            sql += " and reltype=%s"
            params.append(reltype)

        self.__db.execute(sql, params)

        result = []

        for table_name, obj_id in self.__db.fetchall():

            coll = self.collections(id=f"lzdb__{table_name}")

            if coll is None:
                continue

            obj = self.items(
                collection=coll,
                id=obj_id
            )

            if obj is not None:
                result.append(obj)

        return result

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
