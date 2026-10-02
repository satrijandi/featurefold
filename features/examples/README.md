# Example specs

Every example in the authoring reference lives here, so the documentation and the
generator cannot drift apart.
`make examples` parses and expands all of them; CI runs it, so an example that
stops being valid fails the build rather than sitting wrong on a page.

They live in `features/examples/` rather than `features/` deliberately: the
generator only reads `features/*.yml`, so these are checked but never built into
models.

| File | Shows |
|---|---|
| `minimal.yml` | the required keys, and nothing else: including `relations` with its `loaded_at` |
| `all_aggregations.yml` | count, sum, min, max, count_distinct, avg together |
| `composite_entity.yml` | a multi-column entity key |
| `every_optional_key.yml` | conditions, every setting including the business `timezone`, typed entities, a versioned source with its row `key`, both distinct methods, an explicit derivation, exposures |
