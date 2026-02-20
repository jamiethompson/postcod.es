# Phase One Goals – Open Data Street Inference

## Overview

Phase One delivers a reproducible, open-data street inference foundation.

It joins three open datasets to produce:

UPRN → postcode → inferred street name → confidence score

The goal is to establish a measurable, deterministic baseline before adding any enrichment layers.

---

## Primary Goals

### G1. UPRN ↔ Postcode Backbone

Using ONSUD:

- Validate UPRN existence.
- Resolve UPRN → postcode.
- Compute postcode → UPRN counts.
- Attach statistical geography codes for auditing.

Outcome:
A canonical open-data backbone for all further inference.

---

### G2. UPRN ↔ Coordinate Mapping

Using OS Open UPRN:

- Resolve UPRN → coordinates.
- Compute real postcode centroids from actual property points.
- Compute postcode bounding boxes.
- Measure spatial density of postcode units.

Outcome:
Spatially grounded postcode intelligence instead of centroid-only approximations.

---

### G3. Baseline Spatial Street Inference

Using OS Open Roads:

- Resolve each located UPRN to nearest named road.
- Compute distance in metres.
- Assign confidence score using distance bands.
- Record method and dataset provenance.

Outcome:
A deterministic, transparent street inference layer that is spatially coherent and fully traceable.

---

### G4. Postcode-Level Aggregation

From UPRN-level inference:

- Derive unique inferred streets per postcode.
- Count UPRNs per inferred street.
- Identify dominant inferred street.
- Compute distance distribution statistics.

Outcome:
Ability to profile postcode structure (single-street vs multi-street vs mixed).

---

### G5. Quality Baseline Metrics

Compute and store:

- Total UPRNs (ONSUD).
- % of UPRNs with coordinates.
- % resolving to named roads.
- P50, P90, P99 distance to nearest named road.

Outcome:
A quantifiable accuracy baseline to measure future improvements.

---

## Non-Goals (Explicitly Out of Scope)

Phase One does NOT provide:

- Property-number-level determinism.
- Postal-grade validation.
- Authoritative delivery point resolution.
- Address enumeration.
- PPD integration.
- EPC integration.
- LLM-assisted inference.

---

## Success Criteria

Phase One is successful when:

- All three datasets ingest cleanly and are versioned.
- UPRN-level street inference runs deterministically.
- Coverage and distance metrics are recorded.
- Outputs are fully reproducible from release identifiers.
- Limitations are clearly documented.

---

## Strategic Outcome

Phase One establishes:

- A measurable spatial baseline.
- A clean and extensible architecture.
- A defensible open-data street inference layer.
- A platform for future enrichment (PPD, EPC, classification, etc.).

Clarity over cleverness.
Traceability over speed.
Accuracy over assumption.