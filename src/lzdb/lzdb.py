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

import getpass

from .constants import *
from .item import LZDBItem
from .collection import Collection
from .lzdict import lzdict

ACCOUNT_NAME = getpass.getuser()

class LZDB(object):
    __db = None
    __collections = None
    __items = None
    traceon = False

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

            collection = Collection(
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

    def expose(self):
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

            if isinstance(value, LZDBItem):
                value = value.id()

            fields.append(field)
            values.append(value)

        # ------------------------------------------------------------------
        # UPDATE existing row
        # ------------------------------------------------------------------
        if dbitem.id() is not None:

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

                if reltype == LZDB_REL_UNDIRECTED:

                    self.__insertLink(
                        target,
                        dbitem,
                        reltype
                    )

    def newItem(self, collection=None, id=None, **refs):
        # If no collection provided, derive one from virtual PK
        if collection is None:
            temp = LZDBItem(None, **refs)

            ukeys = temp.virtualKeys()
            fkeys = temp.foreignKeys()

            # Try existing collection
            for coll in self.__collections:
                if coll.uniqueKeys() == ukeys:
                    collection = coll
                    break


            # Otherwise create new collection
            if collection is None:
                collection = Collection(self, ukeys=ukeys, fkeys=fkeys, dbitem=None, tname='')
                self.__collections.append(collection)

        # Create item bound to collection
        dbitem = LZDBItem(collection, **refs)
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
        collection = Collection(self, ukeys=ukeys, fkeys=fkeys)
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

