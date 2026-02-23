# Pipeline Architecture Docs

This section explains how datasets relate to each other, how data moves through ingest/stage/build passes, and what value is added at each step.

## Quick Links
- Relationship map + Mermaid system diagram: [`relationships-overview.md`](relationships-overview.md)
- End-to-end value by pass: [`value-added-by-stage.md`](value-added-by-stage.md)
- Dataset index: [`datasets/README.md`](datasets/README.md)
- Stage/pass index: [`stages/README.md`](stages/README.md)

## Authoritative Contracts
- Behavioral spec: [`../spec/pipeline_v3/spec.md`](../spec/pipeline_v3/spec.md)
- Data model: [`../spec/pipeline_v3/data_model.md`](../spec/pipeline_v3/data_model.md)
- Canonicalisation/determinism: [`../spec/pipeline_v3/canonicalisation.md`](../spec/pipeline_v3/canonicalisation.md)

## Reading Order (Fastest Onboarding)
1. [`relationships-overview.md`](relationships-overview.md)
2. [`datasets/README.md`](datasets/README.md)
3. [`stages/README.md`](stages/README.md)
4. [`value-added-by-stage.md`](value-added-by-stage.md)

## Scope Note
- Legacy docs under `docs/spec/phase_1/` and `docs/spec/phase_2-open-names/` are historical.
- For new implementation work, default to V3 docs and this architecture section.
