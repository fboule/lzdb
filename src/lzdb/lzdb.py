import getpass
import inspect
import pprint

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

    def __init__(self, conn, traceon=False):
        self.__conn = conn
        self.__db = conn.cursor()
        self.__collections = []
        self.__items = []
        LZDB.traceon = traceon

        # Ensure core metadata tables are present
        self.__createSystemTables()

        # Load collections
        self.__db.execute("SELECT id, ukeys, tname FROM lzdb")
        tables = self.__db.fetchall()

        for table in tables:
            ukeys = table[1].split(',') if table[1] else []
            collection = Collection(
                self,
                ukeys=ukeys,
                tname=table[2]
            )
            collection._Collection__id = f"lzdb__{table[0]}"
            self.__collections.append(collection)

        # Load foreign keys & items
        for collection in self.__collections:
            collection.read_fkeys(self.__db, collection.id)

        for collection in self.__collections:
            collection.read(self.__db, collection.id)

    @property
    def conn(self):
        return self.__conn

    def expose(self):
        g = inspect.currentframe().f_back.f_globals

        g.update({
            'lzitem': self.ensure,
            'lzitems': self.items,
            'lzc': self.collections,
            'lzcnames': self.collectionsNames,
            'dd': lzdict(),
            'pp': pprint.PrettyPrinter().pprint,
        })

    def ensure(self, **refs):
        matches = self.items(**refs)

        if matches:
            return min(matches, key=lambda item: item.id if item.id is not None else float('inf'))

        return self.newItem(**refs)

    def commit(self):
        self.__createSystemTables()
        self.__createCollections()
        self.__saveItems()
        self.__saveLinks()
        self.__conn.commit()

    def __createSystemTables(self):
        # ENUM type for reltype
        self.__db.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_type WHERE typname = 'lzdb_reltype'
                ) THEN
                    CREATE TYPE lzdb_reltype AS ENUM ('directed', 'undirected');
                END IF;
            END$$;
        """)

        # Collections table
        self.__db.execute("""
            CREATE TABLE IF NOT EXISTS lzdb(
                id SERIAL PRIMARY KEY,
                ukeys VARCHAR UNIQUE,
                tname VARCHAR
            );
        """)

        # Links table using ENUM
        self.__db.execute("""
            CREATE TABLE IF NOT EXISTS lzdb_links(
                src_collection INTEGER NOT NULL,
                src_id INTEGER NOT NULL,
                dst_collection INTEGER NOT NULL,
                dst_id INTEGER NOT NULL,
                reltype lzdb_reltype NOT NULL DEFAULT 'directed',

                UNIQUE(
                    src_collection,
                    src_id,
                    dst_collection,
                    dst_id,
                    reltype
                )
            );
        """)

    def __createCollections(self):
        for collection in self.__collections:
            collection.createTable(self.__db)

    def __saveItem(self, dbitem):
        coll = dbitem.collection

        coll.createNewFields(self.__db, dbitem)

        fields = []
        values = []

        for field in sorted(dbitem.keys()):
            if field == "id":
                continue

            value = dbitem[field]

            if isinstance(value, LZDBItem):
                value = value.id

            fields.append(field)
            values.append(value)

        # UPDATE existing item
        if dbitem.id is not None:
            if len(fields) > 0:
                assignments = [f'"{field}"=%s' for field in fields]
                sql = (
                    f'UPDATE "{coll.id}" '
                    f'SET {", ".join(assignments)} '
                    f'WHERE id=%s'
                )
                params = values + [dbitem.id]
                self.__db.execute(sql, params)

            dbitem.clearDirty()
            return

        # INSERT new item
        if len(fields) == 0:
            sql = (
                f'INSERT INTO "{coll.id}" '
                f'DEFAULT VALUES '
                f'RETURNING id'
            )
            self.__db.execute(sql)
        else:
            placeholders = ", ".join(["%s"] * len(values))
            quoted_fields = ",".join([f'"{f}"' for f in fields])
            sql = (
                f'INSERT INTO "{coll.id}" '
                f'({quoted_fields}) '
                f'VALUES ({placeholders}) '
                f'RETURNING id'
            )
            self.__db.execute(sql, values)

        res = self.__db.fetchone()
        if res is not None:
            dbitem.id = res[0]

        dbitem.clearDirty()

    def __saveItems(self):
        for dbitem in self.__items:
            if dbitem.isDirty:
                self.__saveItem(dbitem)

    def __extract_collection_id(self, collection):
        if not collection or not collection.id:
            return None
        return int(collection.id.split('__')[1])

    def __insertLink(self, src, dst, reltype):
        reltype_str = 'directed' if reltype == LZDB_REL_DIRECTED else 'undirected'

        src_coll_id = self.__extract_collection_id(src.collection)
        dst_coll_id = self.__extract_collection_id(dst.collection)

        if src_coll_id is None or dst_coll_id is None:
            return

        self.__db.execute(
            """
            INSERT INTO lzdb_links(
                src_collection,
                src_id,
                dst_collection,
                dst_id,
                reltype
            )
            VALUES(%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                src_coll_id,
                src.id,
                dst_coll_id,
                dst.id,
                reltype_str
            )
        )

    def __saveLinks(self):
        for dbitem in self.__items:
            for link in getattr(dbitem, 'links', []):
                target = link['item']
                reltype = link['reltype']

                if dbitem.id is None or target.id is None:
                    continue

                self.__insertLink(dbitem, target, reltype)

                if reltype == LZDB_REL_UNDIRECTED:
                    self.__insertLink(target, dbitem, reltype)

    def newItem(self, collection=None, id=None, **refs):
        if collection is None:
            temp = LZDBItem(None, **refs)
            ukeys = temp.virtualKeys
            fkeys = temp.foreignKeys
            collection = self.collections(ukeys=ukeys, fkeys=fkeys)

        dbitem = LZDBItem(collection, **refs)
        self.__items.append(dbitem)

        if id is not None:
            dbitem.id = id

        return dbitem

    def collectionsNames(self):
        return [collection.name() for collection in self.__collections]

    def collections(self, ukeys=None, fkeys=None, id=None, name=None):
        if name is not None:
            for collection in self.__collections:
                if collection.name() == name:
                    return collection
            return None

        if id is not None:
            for collection in self.__collections:
                if collection.id == id:
                    return collection
            return None

        if ukeys is None:
            return self.__collections

        ukeys = sorted(ukeys)
        for collection in self.__collections:
            coll_keys = collection.uniqueKeys
            if coll_keys is not None and sorted(coll_keys) == ukeys:
                return collection

        collection = Collection(self, ukeys=ukeys, fkeys=fkeys)
        self.__collections.append(collection)
        return collection

    def items(self, collection=None, **refs):
        if len(refs) == 0 and collection is None:
            return self.__items

        items = []

        for item in self.__items:
            if collection is not None and item.collection != collection:
                continue

            myitems = {**dict(item.items()), "id": item.id}
            if refs and not (refs.items() <= myitems.items()):
                continue

            items.append(item)

        return items

    def linkedItems(self, item, reltype=None):
        # Always return a list, never None
        src_coll_id = self.__extract_collection_id(getattr(item, 'collection', None))
        if src_coll_id is None or item.id is None:
            return []

        # Build SQL depending on reltype
        if reltype is None:
            sql = """
                SELECT dst_collection, dst_id
                FROM lzdb_links
                WHERE src_collection = %s AND src_id = %s

                UNION

                SELECT src_collection, src_id
                FROM lzdb_links
                WHERE dst_collection = %s AND dst_id = %s
                AND reltype = 'undirected'
            """
            params = [src_coll_id, item.id, src_coll_id, item.id]

        else:
            reltype_str = 'directed' if reltype == LZDB_REL_DIRECTED else 'undirected'

            if reltype == LZDB_REL_DIRECTED:
                sql = """
                    SELECT dst_collection, dst_id
                    FROM lzdb_links
                    WHERE src_collection = %s AND src_id = %s
                    AND reltype = %s
                """
                params = [src_coll_id, item.id, reltype_str]

            else:  # undirected
                sql = """
                    SELECT dst_collection, dst_id
                    FROM lzdb_links
                    WHERE src_collection = %s AND src_id = %s
                    AND reltype = %s

                    UNION

                    SELECT src_collection, src_id
                    FROM lzdb_links
                    WHERE dst_collection = %s AND dst_id = %s
                    AND reltype = %s
                """
                params = [
                    src_coll_id, item.id, reltype_str,
                    src_coll_id, item.id, reltype_str
                ]

        # Use a fresh cursor so committed rows are visible
        cur = self.__conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

        result = []
        for coll_id, obj_id in rows:
            coll = self.collections(id=f"lzdb__{coll_id}")
            if coll is None:
                continue

            matched = self.items(collection=coll, id=obj_id)
            if matched:
                result.extend(matched)

        return result
