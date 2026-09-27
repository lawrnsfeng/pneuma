# Pipeline corpus fixtures

Copied verbatim from the original's fixtures, at commit
`0397327144dcd006cbd783de5f39b053a93f6341`.

These are the **real** pipeline definitions the original service is tested against, not
hand-written approximations. That matters: an invented fixture would silently sidestep exactly
the wire-shape mismatches these tests exist to catch (the bare-string vs. map `start` form, the
`children` key that Rust calls `successors`, `Condition` nodes lacking `name`/`version`/`params`,
and four-level aggregator nesting).

If they are ever refreshed, update the commit hash above so a future divergence is traceable to
"the original definitions changed" rather than "our parsing changed".

| File | What it covers |
|---|---|
| `pipeline1.yaml` | DictAggregator with no Condition child — the shape that cannot complete in the original (see the survey notes) |
| `pipeline2.yaml`, `pipeline3.yaml`, `pipeline4.yaml` | Multi-key aggregator `start` maps |
| `pipeline_case1-3.yaml` | Assorted linear and fan-in shapes |
| `pipeline_condition_list.yaml` | All four condition operators, inside a ListAggregator |
| `pipeline_condition_dict.yaml` | Conditions inside a DictAggregator |
| `pipeline_nested_list_dict.yaml` | Four-level nesting: list-in-list-in-dict, dict-in-dict, list-in-dict-in-list |
| `pipeline_params.yaml` | Pipeline-level multi-key `start`, and node `params` |
