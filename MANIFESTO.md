# Lazy Data Manifesto
*A declaration for schema-emergent data systems*

## 1. Introduction

Modern databases assume that structure is known in advance.
They require schemas, migrations, and predefined relationships before any meaningful data exists.
This assumption is convenient for mature domains — but destructive for exploratory work.

Research, prototyping, and early-stage discovery do not begin with certainty.
They begin with **questions**, **partial understanding**, and **messy data**.

Yet traditional systems punish this natural uncertainty.
They force premature decisions, embed early misconceptions into rigid schemas, and interrupt discovery with migrations.

**Lazy Data Modeling** exists to challenge this paradigm.

It asserts that:

- exploration is a legitimate state of knowledge
- structure should follow discovery
- tools must adapt to the researcher, not the other way around

lzdb is the reference implementation of this philosophy.

---

## 2. The Problem: Premature Structure

Schema-first design creates friction at every stage of exploratory work:

- You must guess the schema before understanding the phenomenon.
- You must perform migrations every time your understanding evolves.
- You must encode early misconceptions into the data model.
- You must treat uncertainty as an error instead of a natural part of discovery.

This friction is not incidental — it is structural.
It is baked into the design of traditional databases.

Lazy Data Modeling rejects this constraint.

---

## 3. The Vision: Schema Emergence

Lazy Data Modeling proposes a simple idea:

> **Let the data arrive first. Let the structure emerge later.**

lzdb embodies this idea by:

- creating tables automatically
- inferring virtual primary keys from inserted data
- evolving schemas lazily as new fields appear
- supporting cross-references without upfront design
- supporting N-to-N relationships without schema changes
- storing relationships in a dedicated system table
- committing changes only when explicitly requested

Structure becomes a *reflection* of understanding, not a prerequisite for it.

---

## 4. The Ethical Argument: Freedom to Explore

Premature structure is a form of coercion.
It forces the researcher to commit to assumptions they may not yet believe.

Lazy Data Modeling defends the researcher’s right to:

- store data without knowing its meaning
- revise understanding without migrations
- let relationships emerge organically
- treat structure as a product of insight

Exploration should not be punished.
Curiosity should not be expensive.

---

## 5. The Method: How Lazy Data Works

Lazy Data Modeling is built on five principles:

### 1. Data first, structure later
Store now. Understand later.
lzdb creates tables only when needed and infers uniqueness from actual data.

### 2. Exploration is a valid state of knowledge
Uncertainty is not a defect.
lzdb allows incomplete, evolving, and heterogeneous data.

### 3. Relationships emerge through use
Cross-references become foreign keys.
Associations become entries in `lzdb_links`.

### 4. Friction is the enemy of insight
Migrations interrupt discovery.
lzdb evolves schemas lazily and automatically.

### 5. Structure follows discovery
Models should reflect understanding, not precede it.

---

## 6. The Tool: lzdb

lzdb demonstrates Lazy Data Modeling through:

### Automatic Table Creation
Each new virtual primary key creates a new table:

```
lzdb__1
lzdb__2
lzdb__3
...
```

The `lzdb` inventory table tracks:

- collection identifiers
- virtual primary keys
- schema metadata

### Virtual Primary Keys
lzdb infers uniqueness from the fields provided during item creation.

Example:

```python
item = dbms.newItem(param='2004', starttime='03-jan-2000', endtime='04-jan-2000')
```

Creates a table with:

```
UNIQUE (endtime, param, starttime)
```

### Cross-References
```python
item2 = dbms.newItem(refers=item1)
```

Creates a foreign key relationship automatically.

### N-to-N Relationships
```python
item1.link(item2)
```

Stored in `lzdb_links`, without modifying any schema.

### Lazy Field Addition
```python
item['clusters'] = [1,2,3]
```

Triggers:

```
ALTER TABLE ADD COLUMN clusters
```

### Explicit Persistence
Nothing is committed until:

```python
dbms.commit()
```

This preserves the exploratory workflow.

---

## 7. The Call to Action

Lazy Data Modeling is an invitation to rethink how we structure data.

It calls for tools that:

- respect uncertainty
- embrace exploration
- evolve with understanding
- remove friction from discovery

lzdb is not the final word — it is the beginning of a movement.

A movement toward **schema-emergent data systems**.
A movement toward **tools that adapt to thought**.
A movement toward **freedom in exploration**.

Join the movement.
Challenge schema-first assumptions.
Let structure follow discovery.

