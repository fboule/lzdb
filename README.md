# lzdb

*A schema-emergent database layer for Python and PostgreSQL.*

## Overview

lzdb (Lazy Database) is a Python persistence layer that allows database structure to emerge from the objects being stored.

Instead of designing tables, foreign keys, and migrations up front, lzdb automatically creates collections, evolves schemas, manages foreign keys, and persists relationships as your application grows.

The guiding principle is:

> Store first. Model later.

lzdb is particularly useful for:

- Research projects
- Scientific data processing
- Exploratory analytics
- Rapid prototyping
- ETL staging systems
- Knowledge graph applications
- Evolving business domains

---

## Main Concepts

### Collections

Objects with the same structure belong to the same collection.

Example:

```python
m1 = lzitem(
    param="2004",
    starttime="03-jan-2000:00:00:00",
    endtime="04-jan-2000:00:00:00"
)

m2 = lzitem(
    param="2005",
    starttime="03-jan-2000:00:00:00",
    endtime="04-jan-2000:00:00:00"
)
```

Both objects belong to the same collection because they share the same schema.

### Collection Signatures

lzdb records a collection signature such as:

```text
endtime,param,starttime
```

The signature determines collection membership.

It does **not** enforce uniqueness.

### Duplicate Rows

Duplicates are allowed.

```python
a = lzitem(name="sat1")
b = lzitem(name="sat1")
```

Both rows are stored independently.

Object identity is provided by PostgreSQL-generated IDs.

---

## Installation

### Install from PyPI

```bash
pip install lzdb2
```

### Build Locally

```bash
python -m build
```

Install:

```bash
pip install dist/*.whl
```

---

## Getting Started

```python
import psycopg as pg
from lzdb import *

conn = pg.connect(
    dbname="test",
    host="localhost"
)

dbms = LZDB(conn)
```

Enable tracing:

```python
dbms = LZDB(conn, traceon=True)
```

---

## Convenience Helpers

Expose shortcuts in the current namespace:

```python
dbms.expose()
```

Creates:

```text
lzitem
lzitems
lzc
lzcnames
dd
pp
```

Example:

```python
sat = lzitem(name="sat1")
```

---

## Creating Objects

```python
sat = dbms.newItem(
    name="SAT1"
)
```

or:

```python
sat = lzitem(name="SAT1")
```

Data is not persisted until:

```python
dbms.commit()
```

---

## ensure()

Retrieve an existing matching object or create one.

```python
sat = dbms.ensure(name="SAT1")
```

Behavior:

```text
0 matches -> create
1 match   -> return object
N matches -> return oldest object
```

This makes object retrieval deterministic.

---

## Foreign Keys

References to lzdb objects automatically become PostgreSQL foreign keys.

```python
sat = lzitem(name="SAT1")

measurement = lzitem(
    satellite=sat,
    timestamp="2025-01-01"
)
```

lzdb automatically creates the FK column and constraint.

After reload:

```python
measurement["satellite"]
```

returns the referenced object.

---

## Automatic Schema Evolution

Objects can gain fields after creation.

```python
sat["operator"] = "ESA"
sat["launch_year"] = 1998
```

During commit, lzdb automatically updates the database schema.

No migrations are required.

---

## Links

Foreign keys represent structural relationships.

Links represent semantic relationships.

### Single Link

```python
sat.link(measurement)
```

### Multiple Links

```python
sat.link([
    measurement1,
    measurement2,
    measurement3
])
```

Links are stored inside the system table:

```text
lzdb_links
```

---

## Relationship Types

```python
LZDB_REL_DIRECTED
LZDB_REL_UNDIRECTED
```

Example:

```python
sat.link(
    measurement,
    LZDB_REL_UNDIRECTED
)
```

---

## Retrieving Links

```python
for item in dbms.linkedItems(sat):
    print(item)
```

---

## Querying

### Retrieve Everything

```python
items = dbms.items()
```

### Filter by Attributes

```python
items = dbms.items(
    param="2004"
)
```

### Filter by Collection

```python
items = dbms.items(
    collection=mycollection
)
```

---

## Realistic Workflow Example

```python
from lzdb import *

measurements = [
    lzitem(param="2004"),
    lzitem(param="2005"),
    lzitem(param="2006")
]

sat = lzitem(name="sat1")

sat.link(measurements)

sat["operator"] = "ESA"
sat["launch_year"] = 1998

dbms.commit()

for item in dbms.linkedItems(sat):
    print(item)
```

---

## lzdict

lzdict provides lazy parquet loading.

```python
dd = lzdict()
```

```python
data = dd["PQTFILE"]
```

---

## Persistence Model

Objects exist in one of three states:

1. New
2. Loaded
3. Dirty

Dirty objects are automatically updated during commit.

---

## Current Dogma

1. Collections emerge from structure.
2. IDs define identity.
3. Duplicate rows are allowed.
4. References become foreign keys.
5. Semantic relationships belong in lzdb_links.
6. Schemas evolve automatically.
7. Nothing is persisted until commit().
8. Existing objects may continue to evolve.

---

## Testing

lzdb includes regression tests covering:

- collection creation
- collection reuse
- duplicate rows
- ensure()
- foreign keys
- foreign-key reload
- graph links
- undirected links
- persistence
- schema evolution

Run:

```bash
pytest -v
```

---

## License

GNU General Public License v3.0 or later (GPL-3.0-or-later).

See the LICENSE file included with the project.
