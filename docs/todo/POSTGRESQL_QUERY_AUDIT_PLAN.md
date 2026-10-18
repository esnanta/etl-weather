# Six-Query PostgreSQL Audit — Pending Acceptance

Branch: `dev`  
Updated: 2026-10-18  
Status: **Query files exist; operator execution and acceptance remain pending.**

The six read-only SQL queries are documented under [`query/README.md`](../../query/README.md). Their implementation and older evidence are not TODO items. Do not modify persisted climate observations or database schema as part of this acceptance check.

## Remaining checks

- [ ] Execute all six query files against the intended `weather` database and retain the results.
- [ ] Validate database/connection identity and actual schema inventory. Do not assume an older five-table inventory; the climate/operational schema now defines seven tables, and geospatial bootstrap defines three additional tables and two views.
- [ ] Cross-check partition summaries against currently persisted observations and the actual campaign stage; do not assume fixed grid-point counts.
- [ ] Check daily quality, UTC+7 boundaries and load evidence. Investigate `FAIL` or `NOT_EVALUATED` without automatically modifying data.
- [ ] Where practical, test SQL detection of missing-day, duplicate, timestamp and metadata mismatch against isolated fixtures.
- [ ] Review query performance again once more historical data are loaded.
- [ ] When accepted, **delete this TODO file**; keep lasting usage instructions in `query/README.md`.

Note: physical PostgreSQL identifiers use the currently configured `APPLICATION_SCHEMA`, `TABLE_PREFIX`, and `VIEW_PREFIX` from `database/config.py`. SQL scripts containing literal physical names may require coordinated updates if prefixes change.
