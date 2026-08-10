# The Lazy Data Modeling Manifesto v1.0

*A founding document for schema-emergent data systems*

## Preamble

For decades, software systems have been built upon a largely unquestioned assumption:

> Before data can be stored, its structure must be known.

This assumption shaped database design, software architecture, project planning, governance, and even the way developers think.

It also created an illusion.

The illusion is that understanding precedes observation.

In reality, understanding is very often the result of observation.

Scientists do not begin with certainty.
Researchers do not begin with certainty.
Explorers do not begin with certainty.
Innovators do not begin with certainty.

They begin with fragments.

They begin with questions.

They begin with incomplete knowledge.

Lazy Data Modeling is founded on the belief that data systems should respect this reality.

---

# Article I: The Right To Explore

Exploration is not a failure to design.

Exploration is a legitimate state of knowledge.

The inability to define a schema in advance is not evidence of poor engineering.

It is often evidence that genuine discovery is occurring.

A data system should therefore support exploration rather than punish it.

When uncertainty exists, the burden should fall on the software, not on the user.

---

# Article II: Data Comes Before Structure

Traditional systems say:

> Understand first. Store later.

Lazy Data Modeling says:

> Store first. Understand later.

Data should never be rejected merely because a complete model does not yet exist.

Structure should emerge from experience.

Structure should be earned.

---

# Article III: Schemas Are Theories

A schema is not truth.

A schema is a hypothesis about reality.

Like all hypotheses, schemas should be allowed to evolve when evidence changes.

Systems that make schema evolution difficult are systems that resist learning.

---

# Article IV: Collections Emerge

Collections must emerge from observation.

A collection is not something declared into existence.

A collection is the recognition that a group of observations share a structure.

Collections therefore become descriptions of reality rather than prescriptions imposed upon reality.

---

# Article V: Identity And Structure Are Different

One of the most common errors in data modeling is the assumption that identical descriptions imply identical things.

Reality frequently disagrees.

Two observations may look identical.

They may still represent different occurrences.

Therefore:

- structure should determine organization
- identity should determine individuality

Collection membership is structural.

Identity is durable.

Structure groups.

Identity distinguishes.

---

# Article VI: Duplicate Observations Are Not A Defect

Many systems are obsessed with eliminating duplicates.

Lazy Data Modeling recognizes that duplicates often carry meaning.

Two identical observations might represent:

- repetition
- verification
- independent measurement
- coincidence
- historical record

Data systems should preserve evidence before attempting to interpret it.

---

# Article VII: Structural Relationships

Some relationships define what an object is.

These relationships are structural.

Example:

```python
event = lzitem(
    satellite=satellite
)
```

Structural relationships deserve structural representation.

They become foreign keys.

---

# Article VIII: Semantic Relationships

Not all relationships belong in schemas.

Some relationships communicate meaning rather than structure.

These are semantic relationships.

Example:

```python
satellite.link(measurement)
```

Schemas describe what something is.

Semantic links describe why something matters.

The distinction is essential.

---

# Article IX: Evolution Is Normal

Knowledge evolves.

Data systems must evolve with it.

Schema evolution should be treated as a routine consequence of learning.

Not as an operational crisis.

Not as a migration project.

Not as a bureaucratic process.

---

# Article X: Discovery Must Be Cheaper Than Migration

A system that makes migration easier than discovery has misplaced its priorities.

Human understanding is the scarce resource.

Schema maintenance is not.

Tools should optimize for insight.

---

# Article XI: Explicit Persistence

Observations become durable when intentionally committed.

```python
dbms.commit()
```

The boundary between thought and permanence should be clear.

---

# Article XII: The Ethics Of Knowledge

Software is never neutral.

Every tool encodes assumptions.

Every workflow privileges certain behaviors.

Lazy Data Modeling chooses to privilege:

- curiosity
- experimentation
- revision
- learning
- adaptation

Systems should help people understand reality.

Systems should not demand certainty before reality has been observed.

---

# The lzdb Dogma

1. Collections emerge from structure.
2. IDs define identity.
3. Duplicate rows are allowed.
4. Object references become foreign keys.
5. Semantic relationships belong in `lzdb_links`.
6. Schemas evolve automatically.
7. Persistence occurs only through `commit()`.
8. Understanding is expected to evolve.

---

# A Declaration

We reject the idea that schemas must precede knowledge.

We reject the idea that uncertainty is failure.

We reject unnecessary friction between observation and persistence.

We affirm that discovery is a first-class activity.

We affirm that structure should emerge naturally.

We affirm that systems should adapt to understanding.

Let structure follow discovery.

---

# License

Copyright (C) 2026 Fabien Bouleau

This document is part of the lzdb project.

Licensed under the GNU General Public License v3.0 or later.
