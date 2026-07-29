# lzdb

LZDB stands for Lazy Database. The database schema is being reworked on-the-fly as data is being committed. The term lazy is used on one hand because the database schema is being implemented only as required and not before injecting data, and, on the other hand, out of laziness, because it doesn't need to be designed upfront.

List of supported features:

 * Instantiating a new item
 * Cross-references between items
 * N-to-N relationships between items
 * Adding/updating fields
 * Automatic schema evolution

## Building the package

The build is done using the standard Python tools, e.g. pypa/build:

```bash
pip install build
```

That will install the build tool, if not already done. Then:

```bash
rm -rf dist
python3 -m build
```

Then it can be installed the usual way:

```bash
pip install dist/*.whl
```

Consequent updates can be performed with:

```bash
pip install dist/*.whl --force-reinstall --no-deps
```

Yeah, using wildcards. Told ya I'm lazy.

## Foreword

Initializing LZDB:

```python
import psycopg as pg
from lzdb import *

LZDB.traceon = True # Optional

dbms = LZDB(
    pg.connect(
        dbname='test',
        host='localhost'
    ),
    traceon=True
)
```

The created tables will be sequentially numbered with the prefix `lzdb__`. The `lzdb` table contains the inventory of all the tables with their virtual primary key. Each table has one primary key which is named `id` and is a sequential number. The virtual primary key is in fact a `unique` declaration. The virtual primary key is on one hand a way to identify duplicates in the table and on the other hand to identify the table.

Let's go for an example in the next section.

**Important note**: everything remains volatile until you explicitly run `dbms.commit()`. An `autocommit` may come in the future.

### The lzdb_links table

LZDB also maintains a system table named `lzdb_links` used to store relationships between existing objects.

The table is created automatically during `dbms.commit()` if it does not already exist.

Its structure is:

```sql
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
```

The `src_collection` and `dst_collection` columns refer to collection identifiers managed through the `lzdb` inventory table.

## Instantiating a new item

```python
item1 = dbms.newItem(
    param='2004',
    starttime='03-jan-2000:00:00:00',
    endtime='04-jan-2000:00:00:00'
)
```

This will create the following table:

```sql
CREATE TABLE IF NOT EXISTS public.lzdb__1
(
    id integer NOT NULL DEFAULT nextval('lzdb__1_id_seq'::regclass),
    endtime character varying,
    param character varying,
    starttime character varying,
    CONSTRAINT lzdb__1_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__1_endtime_param_starttime_key UNIQUE (endtime, param, starttime)
)
```

In the `lzdb` table, the following record will be inserted:

 * id: 1
 * ukeys: endtime,param,starttime

Each subsequent item created with the same virtual primary key will end up in the table with id 1.

## Cross-references between items

Let's go with an example:

```python
item2 = dbms.newItem(refers=item1)
```

This will create a second table with `refers` as virtual primary key and declare the field as foreign key as follows:

```sql
CREATE TABLE IF NOT EXISTS public.lzdb__2
(
    id integer NOT NULL DEFAULT nextval('lzdb__2_id_seq'::regclass),
    refers integer,
    CONSTRAINT lzdb__2_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__2_refers_key UNIQUE (refers),
    CONSTRAINT lzdb__2_refers_fkey FOREIGN KEY (refers)
        REFERENCES public.lzdb__1 (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
```

The inserted record will look like the following:

 * id: 1
 * refers: 1

### Cross-reference versus relationship

LZDB supports two ways of connecting objects:

#### Cross-reference

```python
item2 = lzitem(refers=item1)
```

Characteristics:

 * Stored as a foreign key.
 * One object explicitly references another.
 * The schema of the collection is modified.
 * Suitable when the reference is part of the object's data model.

#### Relationship

```python
item1.link(item2)
```

Characteristics:

 * Stored in `lzdb_links`.
 * Supports N-to-N relationships.
 * Does not modify the schema of either collection.
 * Suitable for arbitrary associations between existing objects.

As a rule of thumb, use a cross-reference when the relationship belongs to the object definition itself and use `link()` when expressing associations between existing objects.

## Relationships between items

LZDB supports N-to-N relationships between arbitrary items.

Unlike cross-references, which are implemented as foreign keys stored directly in the collection tables, relationships are stored in the dedicated `lzdb_links` table.

This allows objects from unrelated collections to be connected without modifying any existing schema.

### Creating relationships

Example:

```python
items = []

items += [
    lzitem(
        param='2004',
        starttime='03-jan-2000:00:00:00',
        endtime='04-jan-2000:00:00:00'
    )
]

items += [
    lzitem(
        param='2005',
        starttime='03-jan-2000:00:00:00',
        endtime='04-jan-2000:00:00:00'
    )
]

items += [
    lzitem(
        param='2006',
        starttime='03-jan-2000:00:00:00',
        endtime='04-jan-2000:00:00:00'
    )
]

sat = lzitem(name='sat1')

sat.link(items)

dbms.commit()
```

This creates the following logical relationships:

```text
sat1 --> 2004
sat1 --> 2005
sat1 --> 2006
```

The links are automatically persisted in the `lzdb_links` system table.

### Relationship types

LZDB currently supports the following relationship types:

```python
LZDB.REL_DIRECTED   = 0
LZDB.REL_UNDIRECTED = 1
```

Directed relationship:

```python
sat.link(item)
```

Undirected relationship:

```python
sat.link(
    item,
    reltype=LZDB.REL_UNDIRECTED
)
```

Undirected relationships are internally stored in both directions, allowing relationship traversal from either side.

### Retrieving linked items

To retrieve all items linked to a given object:

```python
for item in dbms.linkedItems(sat):
    print(item['param'])
```

Output:

```text
2004
2005
2006
```

Relationships can also be filtered by type:

```python
links = dbms.linkedItems(
    sat,
    reltype=LZDB.REL_DIRECTED
)
```

## Adding fields to an item

Now, let's attach some data to the records. There are two syntaxes possible.

The dict-way:

```python
item2['clusters'] = [1,2,3]
item2['freqmap'] = [4,5,6]
```

The `set` method:

```python
item2.set(
    clusters=[1,2,3],
    freqmap=[4,5,6]
)
```

Since the table `lzdb__2` has already been created, it will be altered with the `ADD COLUMN` statement.

The table will then have the following definition:

```sql
CREATE TABLE IF NOT EXISTS public.lzdb__2
(
    id integer NOT NULL DEFAULT nextval('lzdb__2_id_seq'::regclass),
    refers integer,
    clusters character varying,
    freqmap character varying,
    CONSTRAINT lzdb__2_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__2_refers_key UNIQUE (refers),
    CONSTRAINT lzdb__2_refers_fkey FOREIGN KEY (refers)
        REFERENCES public.lzdb__1 (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
```

The record will be **updated** as follows:

 * id: 1
 * refers: 1
 * clusters: [1,2,3]
 * freqmap: [4,5,6]

If the value of an existing field is changed, the record in the database will simply be updated.

## For a little more laziness

The LZDB class comes with a `register` method that will put in place a couple of functions to be used as shortcuts to the methods.

It is implicitly called and works the following way:

```python
import psycopg as pg
from lzdb import *

dbms = LZDB(
    pg.connect(
        dbname='test',
        host='localhost'
    ),
    traceon=True
)

item1 = lzitem(
    param='2004',
    starttime='03-jan-2000:00:00:00',
    endtime='04-jan-2000:00:00:00'
)
```

They all start with `lz` and map to the following methods:

 * lzitem: newItem
 * lzc: collections
 * lzcnames: collectionNames
 * lzitems: items

Also, to read parquet data, the lzdict class has been introduced:

```python
dd = lzdict()
mydata = dd['PQTFILE']
```

This will search in the data/ subfolder for a file matching the pattern `*PQTFILE*`, load it and keep it in the dictionary, and finally return the element.

```python
>>> dd.keys()
dict_keys(['PQTFILE'])
```

Also note that the `dd` variable is already defined as:

```python
dd = lzdict()
```

and the `pp` variable points to the pretty print function:

```python
pprint.PrettyPrinter().pprint
```

Example:

```python
>>> items = dbms.items(param='2004')
>>> pp(items)

[{'endtime': '04-jan-2000:00:00:00',
  'id': 1,
  'param': '2004',
  'starttime': '03-jan-2000:00:00:00'},
 {'endtime': '05-jan-2000:00:00:00',
  'id': 2,
  'param': '2004',
  'starttime': '04-jan-2000:00:00:00'}]
```
