# The Lazy Data Manifesto

*A declaration of independence from schema-first thinking*

---

# We Reject Premature Certainty

For half a century, database systems have been built on a hidden assumption:

> The structure of knowledge is known before the knowledge itself exists.

This assumption has become so common that it is rarely questioned.

Before storing data, we are asked to define:

- tables
- entities
- relationships
- constraints
- migrations
- governance rules

before we have fully observed the phenomenon we wish to understand.

This may be appropriate for mature domains.

It is not appropriate for discovery.

Research does not begin with certainty.

Science does not begin with certainty.

Innovation does not begin with certainty.

Curiosity does not begin with certainty.

They begin with questions.

Lazy Data Modeling begins with the same questions.

---

# The Great Inversion

Traditional data modeling says:

> Understand first. Store later.

We propose the opposite:

> Store first. Understand later.

Knowledge should not wait for perfect structure.

Structure should earn its existence.

A schema is not a starting point.

A schema is a theory.

And theories should emerge from observation.

---

# The Tyranny of the Schema

Schema-first systems often force developers, analysts, and researchers to commit to assumptions long before those assumptions deserve commitment.

Every premature schema becomes a prediction.

Every prediction eventually collides with reality.

The result is:

- migrations
- redesigns
- rewrites
- brittle software
- lost time

The problem is not incompetence.

The problem is the belief that uncertainty can be designed away.

It cannot.

---

# Exploration Is Not Failure

Many systems treat uncertainty as a defect.

We reject this idea.

Uncertainty is evidence that discovery is occurring.

Exploration is not the absence of knowledge.

Exploration is the process by which knowledge is created.

Any data system that punishes exploration will ultimately punish discovery.

---

# Structure Must Follow Discovery

The purpose of a persistence system is not to enforce assumptions.

The purpose of a persistence system is to preserve observations while understanding evolves.

Therefore:

- collections should emerge
- fields should emerge
- references should emerge
- relationships should emerge

Structure should be a consequence.

Never a prerequisite.

---

# Identity Is Not Structure

One of the most dangerous mistakes in data modeling is the belief that identical descriptions imply identical things.

Reality does not work that way.

Two observations may be indistinguishable.

They may still be different observations.

Lazy Data Modeling therefore separates:

- identity
- structure

Structure determines collection membership.

Identity is determined by a durable identifier.

Duplicate observations are allowed.

Reality is not obligated to be unique.

---

# Collections Are Observations About Data

In lzdb, a collection is not designed.

A collection is discovered.

Objects with compatible structure naturally belong together.

Collections emerge because structure emerges.

Not because a designer declared them into existence.

---

# Structural And Semantic Relationships

Not every connection means the same thing.

Some relationships define structure.

Some relationships express meaning.

A foreign key says:

> This object depends upon another object.

A semantic link says:

> These objects matter together.

Confusing these two ideas has polluted data models for decades.

Lazy Data Modeling treats them separately.

Structure deserves structure.

Meaning deserves freedom.

---

# Evolution Is Normal

A schema that never changes is not evidence of wisdom.

It is often evidence that curiosity stopped.

Understanding evolves.

Data systems should evolve with it.

Adding a field should feel like recording a discovery.

Not filing paperwork.

---

# The Ethics Of Uncertainty

There is an ethical dimension to software design.

Tools either support thinking or constrain thinking.

Tools either encourage curiosity or suppress curiosity.

Tools either welcome uncertainty or punish uncertainty.

We believe researchers, developers, analysts, and explorers deserve tools that respect the reality of incomplete knowledge.

No system should force commitment before understanding.

---

# The Principles Of Lazy Data Modeling

1. Data comes before structure.
2. Exploration is a valid state of knowledge.
3. Collections emerge from observation.
4. Identity is separate from structure.
5. Duplicate observations are allowed.
6. Structural and semantic relationships are different.
7. Schemas must evolve naturally.
8. Persistence should remain explicit.
9. Discovery should be easier than migration.
10. Understanding should drive structure.

---

# The Current lzdb Dogma

1. Collections emerge from structure.
2. IDs define identity.
3. Duplicate rows are allowed.
4. Object references become foreign keys.
5. Semantic relationships belong in `lzdb_links`.
6. Schemas evolve automatically.
7. Persistence occurs only through `commit()`.
8. Understanding is expected to evolve.

---

# A Call To Builders

Build systems that learn.

Build systems that adapt.

Build systems that tolerate uncertainty.

Build systems that preserve discovery.

Challenge the assumption that every question requires a schema.

Challenge the assumption that every relationship requires a model.

Challenge the assumption that certainty must come first.

The future belongs to systems that follow thought rather than constrain it.

Let structure follow discovery.

---

# License

Copyright (C) 2026 Fabien Bouleau

This document is part of the lzdb project.

Licensed under the GNU General Public License v3.0 or later.
