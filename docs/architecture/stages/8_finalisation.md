# Pass 8: Finalisation

## Purpose
Resolve candidate evidence into final postcode/street outputs and materialized API projections.

## Inputs
- `derived.postcode_street_candidates`
- frequency weights config

## Outputs
- `derived.postcode_streets_final`
- `derived.postcode_streets_final_candidate`
- `derived.postcode_streets_final_source`
- versioned API tables:
  - `api.postcode_street_lookup__<dataset_version>`
  - `api.postcode_lookup__<dataset_version>`

## Deterministic Probability
- exact formula normalization by postcode total weight
- fixed-scale rounding + deterministic residual correction to rank 1 street

## Value Added
- converts evidence graph into stable product outputs
- provides reproducible hashes and publishable API projections

## Related
- Probability contract: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
- Canonicalisation: [`../../spec/pipeline_v3/canonicalisation.md`](../../spec/pipeline_v3/canonicalisation.md)
