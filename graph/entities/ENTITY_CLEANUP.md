# Entity Cleanup Workflow

Use only these artifacts for cleanup:

1. `graph/entities/check_entities_report.txt`
2. `graph/entities/entity_cleanup_queue.sql`
3. `graph/entities/entity_decisions.tsv`

## Loop

1. Refresh spans in `ati_verses.ner_span`.
2. Run full audit only when needed:
   - `psql -d tipitaka -f graph/entities/audit_missing_entity_resolution.sql > graph/entities/check_entities_report.txt`
3. Use compact queue for daily triage:
   - `psql -d tipitaka -f graph/entities/entity_cleanup_queue.sql`
4. For each decision, append one row to `graph/entities/entity_decisions.tsv`.
5. Apply DB changes in batches, then rerun step 3.

## Decision Values

`resolution` should be one of:
- `add_canonical`
- `add_alias`
- `relabel_entity`
- `ignore_noise`
- `defer`

## Guardrails

- Do not create new ad-hoc report files.
- Keep all rationale in `entity_decisions.tsv`.
- If you need verse evidence, store verse refs in the `note` field, not new text files.
