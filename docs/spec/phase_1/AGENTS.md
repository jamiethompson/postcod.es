# docs/spec/phase_1/AGENTS.md

This directory contains the authoritative documentation for **Phase 1** of the pipeline.

If you are making changes to Phase 1 code, you are expected to read and respect the documents in this folder.

Phase 1 is about correctness, determinism, and measurable baseline behaviour.
Not cleverness. Not scope creep.

---

# Documents in this Folder

## goals.md

Defines:

- What Phase 1 delivers
- What it explicitly does not deliver
- Success criteria

Use this to decide whether proposed work is in scope.

If your change alters scope, update `goals.md` in the same change.

---

## prd.md

Defines:

- Why Phase 1 exists
- Functional requirements
- Non-functional requirements
- Non-goals
- Risks
- Acceptance criteria

Use this to validate product intent and ensure no silent requirement drift.

If behaviour changes, confirm it still satisfies the PRD.
If it doesn’t, update the PRD deliberately.

---

## spec.md

Defines:

- Data model
- Normalisation rules
- Spatial inference rules
- Confidence scoring
- Determinism guarantees
- Provenance requirements
- Metrics

This is the behavioural contract.

If code behaviour differs from `spec.md`, one of them is wrong.
Resolve it immediately and update the documentation accordingly.

Do not allow undocumented behaviour.

---

## ../data_sources.md

Defines:

- Official dataset URLs
- Licence terms
- Update frequency expectations

If dataset acquisition changes, URLs move, or licence terms differ,
update this document.

If something is unknown, write **Unknown** and add a note to verify.
Do not guess.

---

## changes.md

This is mandatory.

It records **behavioural deviations from the original Phase 1 specification**.

It is not a commit log.
It is not a changelog of minor edits.
It is not optional.

Log entries are required when changing:

- Confidence scoring thresholds
- Search radius
- Normalisation rules
- Join logic
- Determinism rules
- Schema outputs
- Metrics definitions
- Dataset assumptions

Each entry must include:

- Date
- Change ID
- What changed
- Why it changed
- Before behaviour
- After behaviour
- Observed or expected metric impact
- Confirmation that spec.md was updated

If behaviour changes and `changes.md` is not updated, the change is incomplete.

---

# Workflow Rules

## Before Starting Work

1. Read `goals.md` to confirm scope.
2. Read `prd.md` to confirm requirements.
3. Read `spec.md` to understand current behaviour.
4. Confirm that your change is necessary and justified.

If the change shifts scope or behaviour, update the docs first.

---

## During Implementation

- Do not introduce implicit assumptions.
- Do not introduce silent fallback logic.
- Do not weaken determinism.
- Do not remove provenance fields.

Fail fast on invalid schema or missing columns.
Partial silent success is not acceptable.

---

## After Implementation

You must:

- Update `spec.md` if behaviour changed (clearly note that a change ahs taken place and timestamp it).
- Update `changes.md` if behaviour deviated.
- Update `goals.md` or `prd.md` if scope or requirements changed.
- Confirm determinism still holds.

If rebuild output changes, document why.

---

# Quality Standards

Phase 1 must remain:

- Deterministic
- Rebuildable from raw inputs
- Fully traceable to dataset release identifiers
- Explicit about limitations
- Transparent in metrics

If a change reduces clarity, traceability, or reproducibility, it should not be merged.

---

# Scope Discipline

Phase 1 is:

- UPRN ↔ postcode backbone
- UPRN ↔ coordinate join
- Nearest named road inference
- Distance-based confidence
- Quality metrics

Phase 1 is not:

- Property-level determinism
- Postal-grade validation
- Address enumeration
- Proprietary dataset reconstruction
- LLM augmentation

If you feel tempted to add “just one extra thing”, stop.
Check `goals.md`.

---

# Decision Rule

If a proposed change:

- Obscures provenance
- Introduces non-determinism
- Expands scope without documentation
- Changes outputs without logging
- Weakens clarity

It should not proceed.

Clarity over cleverness.
Traceability over speed.
Controlled evolution over drift.