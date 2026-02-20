# AGENTS.md

This repository contains a **data import and transformation pipeline** for UK open datasets.
Its purpose is to produce a reproducible, versioned derived dataset:

UPRN → postcode → inferred street name → confidence score

This file defines behavioural rules, quality standards, and documentation requirements
for any agent contributing to this project.

The priority is **accuracy, provenance, and reproducibility**.

---

## 1. Core Principles

### 1.1 No Guessing
If a dataset field, schema, release identifier, or licence detail is unknown:
- Mark it as **Unknown**
- Add validation logic
- Document the assumption explicitly

Never silently assume structure based on “typical” formats.

---

### 1.2 Reproducibility First
The pipeline must be:
- Deterministic
- Rebuildable from raw inputs
- Fully traceable to dataset release identifiers

If the same inputs are used, outputs must be identical.

No hidden state.
No environment-dependent logic.
No implicit defaults.

---

### 1.3 Raw Data is Sacred
- Raw imports are immutable.
- Transformations must not mutate raw tables.
- Derived outputs must be rebuildable from raw + release metadata.

If you need to correct something, rebuild it — do not patch it.

---

### 1.4 Provenance is Mandatory
Every derived dataset must clearly record:
- Source dataset release identifiers
- Method used
- Computation timestamp

If provenance is not recorded, the output is invalid.

---

### 1.5 Explicit Limitations
Street inference is:
- Heuristic
- Distance-based
- Non-authoritative

Documentation must clearly state this.
Do not imply authoritative delivery-level correctness.

---

## 2. Documentation Requirements

Every meaningful change must include documentation updates.

At minimum:

### 2.1 Dataset Documentation
Maintain a living document describing:
- Each dataset
- Where it is obtained
- Licence type
- Required fields
- Known limitations
- Known schema quirks

If a dataset changes, update the documentation immediately.

---

### 2.2 Data Model Documentation
Maintain clear documentation for:
- Raw tables
- Core tables
- Derived tables
- Metrics tables

Include:
- Field definitions
- Data types
- Constraints
- Semantic meaning

No column should exist without documented purpose.

---

### 2.3 Transform Documentation
For each transformation layer, document:
- Inputs
- Outputs
- Assumptions
- Failure modes
- Determinism guarantees

If logic changes (e.g., confidence thresholds), update documentation and record the change rationale.

---

### 2.4 Metrics Documentation
Define:
- What each metric measures
- How it is calculated
- Why it exists
- Expected ranges

Metrics are part of product quality, not optional extras.

---

## 3. Quality Standards

### 3.1 Deterministic Behaviour
- Stable ordering in queries
- Explicit tie-breaking rules
- No reliance on implicit database ordering

### 3.2 Observability
Each pipeline run must:
- Log row counts per stage
- Log join coverage percentages
- Log resolution percentages
- Log distance percentiles

Silent processing is not acceptable.

---

### 3.3 Fail Fast
If:
- Required columns are missing
- Geometry is invalid
- Coordinate reference systems are inconsistent

The pipeline must fail clearly.

Partial silent success is worse than failure.

---

### 3.4 Schema Validation
Before processing:
- Validate required fields exist
- Validate types where possible
- Record dataset release metadata

Do not infer schema dynamically without documentation.

---

### 3.5 No Scope Drift
This repository is a **pipeline**, not:
- An API
- A serving layer
- An analytics platform
- A proprietary dataset reconstruction engine

Keep scope disciplined.

---

## 4. Testing Expectations

Agents must ensure:

- Normalisation logic is tested.
- Derived outputs are deterministic.
- Schema validation works.
- Metrics calculations are stable.
- Small fixture datasets validate spatial inference logic.

Tests must:
- Use synthetic or reduced fixture data.
- Not depend on downloading live datasets.

---

## 5. Change Management

Any change to:
- Confidence scoring
- Search radius
- Join logic
- Normalisation rules
- Spatial reference systems

Must include:

1. Rationale
2. Before/after metrics comparison
3. Determinism confirmation
4. Documentation update

---

## 6. What Must Never Be Implemented Here

- Address enumeration features
- Proprietary dataset integration
- Undocumented inference layers
- Hidden optimisation logic
- Behaviour designed for ambiguous or non-transparent use cases

This pipeline exists to:
- Normalise open data
- Join open data
- Derive transparent street-level inference
- Record quality metrics

Nothing more.

---

## 7. Communication Standards

Pull requests must:

- State the problem being solved
- Describe the solution
- Document assumptions
- Include metric impact
- Confirm reproducibility

Avoid vague language such as:
- “Seems to work”
- “Probably correct”
- “Should be fine”

Be precise.

---

## 8. Decision Rule

If a proposed change:
- Reduces transparency,
- Obscures provenance,
- Makes outputs less reproducible,
- Or introduces implicit assumptions,

It should not be merged.

Clarity over cleverness.
Traceability over speed.
Correctness over convenience.

---

End of AGENTS.md