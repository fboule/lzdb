# lzdb
*A schema-emergent database layer for Python and PostgreSQL.*

**lzdb** (Lazy Database) is a datastore that **builds and evolves its schema automatically** as data is inserted.
Instead of designing tables upfront, lzdb lets structure **emerge naturally from the data itself**.

This approach is part of the **Lazy Data Modeling** philosophy:

- store first, understand later
- let relationships appear through use
- avoid premature schema design
- embrace uncertainty during exploration

lzdb is ideal for research workflows, prototypes, and dynamic data environments where the schema cannot be known in advance.

---

## Key Features

- **Automatic schema evolution**
- **Virtual primary keys** inferred from inserted data
- **Cross-references** implemented as foreign keys
- **N-to-N relationships** via a dedicated system table
- **Automatic table creation** (`lzdb__N`)
- **Lazy field addition** (`ALTER TABLE ADD COLUMN`)
- **Convenience API** (`lzitem`, `lzitems`, `lzdict`, etc.)

---

## Installation

Build using standard Python packaging tools:

```bash
pip install build
```

Then:

```bash
rm -rf dist
python3 -m build
```

Install the wheel:

```bash
pip install dist/*.whl
```

Force-reinstall for updates:

```bash
pip install dist/*.whl --force-reinstall --no-deps
```

Yes, using wildcards. Told ya I'm lazy.

---

## Foreword: Initializing LZDB

```python
import psycopg as pg
from lzdb import *

LZDB.traceon = True  # Optional

dbms = LZDB(
    pg.connect(
        dbname='test',
        host='localhost'
    ),
    traceon=True
)
```

### Table Naming

lzdb creates tables named:

```
lzdb__1
lzdb__2
lzdb__3
...
```

The `lzdb` inventory table stores:

- the virtual primary key (unique fields)
- the collection identifier
- schema metadata

Each table has:

- a real primary key: `id`
- a virtual primary key: a `UNIQUE` constraint over inferred fields

### Important Note

Nothing is persisted until you call:

```python
dbms.commit()
```

(An `autocommit` may come later.)

---

## The `lzdb_links` Table

Relationships between objects are stored in a dedicated system table:

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

This table supports:

- directed relationships
- undirected relationships
- N-to-N associations
- cross-collection links

It is created automatically during `dbms.commit()`.

---

## Instantiating a New Item

```python
item1 = dbms.newItem(
    param='2004',
    starttime='03-jan-2000:00:00:00',
    endtime='04-jan-2000:00:00:00'
)
```

This produces:

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

And inserts into `lzdb`:

- id: 1
- ukeys: endtime,param,starttime

Any item with the same virtual primary key goes into the same table.

---

## Cross-References Between Items

```python
item2 = dbms.newItem(refers=item1)
```

This creates a second table:

```sql
CREATE TABLE IF NOT EXISTS public.lzdb__2
(
    id integer NOT NULL DEFAULT nextval('lzdb__2_id_seq'::regclass),
    refers integer,
    CONSTRAINT lzdb__2_pkey PRIMARY KEY (id),
    CONSTRAINT lzdb__2_refers_key UNIQUE (refers),
    CONSTRAINT lzdb__2_refers_fkey FOREIGN KEY (refers)
        REFERENCES public.lzdb__1 (id)
)
```

Inserted record:

- id: 1
- refers: 1

---

## Cross-Reference vs Relationship

### Cross-reference

```python
item2 = lzitem(refers=item1)
```

Characteristics:

- stored as a foreign key
- modifies the schema
- part of the object’s definition

### Relationship

```python
item1.link(item2)
```

Characteristics:

- stored in `lzdb_links`
- supports N-to-N
- does not modify schema
- ideal for arbitrary associations

**Rule of thumb:**
Use cross-references for structural relationships.
Use `link()` for semantic relationships.

---

## N-to-N Relationships

Example:

```python
items = [
    lzitem(param='2004', starttime='03-jan-2000:00:00:00', endtime='04-jan-2000:00:00:00'),
    lzitem(param='2005', starttime='03-jan-2000:00:00:00', endtime='04-jan-2000:00:00:00'),
    lzitem(param='2006', starttime='03-jan-2000:00:00:00', endtime='04-jan-2000:00:00:00')
]

sat = lzitem(name='sat1')

sat.link(items)

dbms.commit()
```

Logical relationships:

```
sat1 --> 2004
sat1 --> 2005
sat1 --> 2006
```

### Relationship Types

```python
LZDB.REL_DIRECTED   = 0
LZDB.REL_UNDIRECTED = 1
```

Undirected relationships are stored in both directions.

### Retrieving Linked Items

```python
for item in dbms.linkedItems(sat):
    print(item['param'])
```

Output:

```
2004
2005
2006
```

---

## Adding Fields to an Item

Two syntaxes:

### Dict-style

```python
item2['clusters'] = [1,2,3]
item2['freqmap'] = [4,5,6]
```

### Method-style

```python
item2.set(
    clusters=[1,2,3],
    freqmap=[4,5,6]
)
```

lzdb automatically performs:

```sql
ALTER TABLE lzdb__2 ADD COLUMN clusters character varying;
ALTER TABLE lzdb__2 ADD COLUMN freqmap character varying;
```

Record becomes:

- id: 1
- refers: 1
- clusters: [1,2,3]
- freqmap: [4,5,6]

Updates simply modify the row.

---

## Laziness Helpers: `expose()`

lzdb installs convenience functions:

- `lzitem` → `newItem`
- `lzc` → `collections`
- `lzcnames` → `collectionNames`
- `lzitems` → `items`

Example:

```python
item1 = lzitem(
    param='2004',
    starttime='03-jan-2000:00:00:00',
    endtime='04-jan-2000:00:00:00'
)
```

### Parquet Loader: `lzdict`

```python
dd = lzdict()
mydata = dd['PQTFILE']
```

This loads any file matching `*PQTFILE*` from `data/`.

```python
>>> dd.keys()
dict_keys(['PQTFILE'])
```

Pretty-print helper:

```python
pp = pprint.PrettyPrinter().pprint
```

Example:

```python
>>> items = dbms.items(param='2004')
>>> pp(items)
```

---

## Why Lazy Data Modeling?

For the full conceptual foundation behind lzdb, see **[Why Lazy Data Modeling?](MANIFESTO.md)**.

---

## License

MIT License
Copyright (c) 2026
