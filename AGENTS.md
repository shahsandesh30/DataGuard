# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Commands

The system `python` on this machine has no dependencies installed. Use the venv interpreter:

```bash
./venv/Scripts/python.exe -m pytest              # full suite (89 tests)
./venv/Scripts/python.exe -m pytest tests/test_storage.py
./venv/Scripts/python.exe -m pytest tests/test_storage.py::test_is_s3_distinguishes_backends
./venv/Scripts/python.exe -m ruff check pipelines dashboard tests
./venv/Scripts/python.exe -m ruff check pipelines dashboard tests --fix
```

Pytest and ruff config live in `pyproject.toml` (there is no `pytest.ini`). Ruff rules are
pinned there deliberately so every laptop reports the same counts.

`.github/workflows/ci.yml` runs the full suite, Ruff, and a Glue ZIP import check on every push
and pull request using Python 3.11. Still run tests and lint locally before handing work over.

Running the pipeline:

```bash
python -m pipelines run --locations 2178 --start 2023-01-01 --end 2023-01-31
python -m pipelines ingest|conform|quality|detect|fuse   # each stage standalone
streamlit run dashboard/app.py
```

`.env` (gitignored) decides where every zone lives, and it currently points at the local
`data/` lake. Uncommenting its `s3://` lines makes **every** command hit AWS silently — there
is no prompt and no dry-run. Check `.env` before running a stage if billing matters.

Console output contains `—` and `µ`, which the Windows console emits as bytes that make
`grep` treat the stream as binary. Use `grep -a` when filtering stage output.

## Architecture

Bronze → silver → gold medallion lake. Each CLI stage reads the zone the previous stage
wrote; there is no hidden state between them.

```
ingest   OpenAQ public S3 -> bronze        raw csv.gz, unchanged, plus an arrival manifest
conform  bronze           -> silver        canonical units/names/timestamps, nothing dropped
quality  silver + bronze  -> gold/layer1   Layer 1: is the data healthy?
detect   bronze           -> gold/layer2   Layer 2: did something happen?
fuse     gold             -> gold/fusion   join on (station, day), escalate or quarantine
```

`docs/pipeline.md` traces the whole run stage by stage, with the function names and the
row counts to expect at each hop.

The two detectors run **independently** — that is the whole premise. Fusion joins them and
applies a severity penalty (`trust = alert_score × (1 − penalty)`, 0.2/0.4/0.7 for
low/medium/high). Any Layer 1 incident on the same station-day flips the status to
`quarantined`. Quarantined alerts are held for review, never deleted — this is public-safety
data, and the dashboard always shows them.

Layer 1 reads **both** silver and bronze: file lateness, schema drift and file presence are
properties of the delivered file, not of the readings inside it.

### Zone roots are strings, not Paths

A zone root is a plain string. `data/silver` is a local directory; `s3://bucket/silver` is an
S3 prefix. `pipelines/storage.py` dispatches on the `s3://` scheme and is the only module that
touches `pathlib` or `boto3` for zone I/O. There is no `--target aws` flag — point a root at S3
and that zone lives in S3. Zones are independent (S3 bronze with local gold is valid).

**`root / "file"` is a bug.** Use `storage.join(root, "file")`. The tests used to pass only
`Path` objects while the CLI passes strings, which hid exactly this crash;
`tests/test_pipeline_end_to_end.py::test_stages_accept_string_roots` guards it now.

Gold tables are written with `storage.write_parquet(frame, root, name)` and read with
`storage.read_parquet`. There is no separate gold module — a `pipelines/gold.py` that only
forwarded to storage was removed as an unnecessary hop.

Writes to S3 register the dataset in the Glue Catalog so Athena sees it with no crawler run.
Reads go straight to S3, not through Athena — same bytes, no workgroup or query-output bucket
needed. Glue table names are derived from the **S3 key**, not the caller's dataset argument
(`s3://<gold>/layer2/event_features` → `layer2_event_features`), because including the layer
prefix prevents equally named datasets in different layers from colliding.

Silver registers as `silver_data` to give Athena users a stable table name. Scheduled stages
read the underlying S3 data directly through `pipelines.storage`.

### Layer 2 detection

- `config.DEFAULT_locationidS` has odd casing that cannot be fixed — `detection/features.py`
  imports it under that spelling.
- `build_detection` reads materialised silver and writes both gold datasets through the shared
  storage layer. Its feature and alert readers are the canonical local/S3 readers used by
  fusion and the dashboard.
- `agreement_count` is the sum of the IF, LOF and DBSCAN boolean votes. `alert_score` is kept
  in `[0, 1]`, and only rows with at least one vote enter the alert table.
- `pipelines/detection/io.py` is only an optional Athena helper for the Layer 2 EDA script; it
  is not part of scheduled execution.

### Partitioning

Both backends partition by `locationid=<ID>/year=<YYYY>`. `year` is derived and lives only in
the path; `locationid` stays **in the local parquet file as well** so local files and S3
datasets retain the same logical schema. `storage.read_parquet` reconstructs partition columns
for local reads (awswrangler does it for S3) and coerces `locationid` back to `Int64`.

### Silver schema

12 columns: `locationid`, `sensor_id`, `location_name`, `datetime` (UTC), `datetime_local`,
`date_local`, `latitude`, `longitude`, `parameter`, `unit`, `value`, `original_unit`.

Readings are conformed but **never dropped or clipped** — negative concentrations and stuck
values must survive into silver for Layer 1 to find them. `original_unit` is kept so Layer 1
can distinguish a converted value from a mislabelled one (rule R10).

Bronze column aliases (`conform.COLUMN_ALIASES`) are keyed on the provider column name
**lowercased with underscores stripped**, so one entry covers `location_id` / `locationId` /
`LOCATIONID`. A missing entry means the fallback keeps the original name, the required-column
check fails, and every file conforms to zero rows behind a warning log — this shipped
undetected for a long time.

### Documentation map

| File | When to read it |
|---|---|
| `docs/pipeline.md` | Tracing a run: which file executes at each step, and the row counts to expect |
| `docs/architecture.md` | Why the system is shaped this way; the known architectural gaps |
| `docs/data-source.md` | OpenAQ archive layout, real column spellings, degradation events E1-E8 |
| `models/*_model_card.md`, `models/fusion_spec.md` | Model features, hyperparameters, and the limitations each layer admits to |

`docs/risk-register.md` numbers **project risks** R1-R7. These are unrelated to the Layer 1
**quality rule** ids R1-R10 in `pipelines/quality/rules.py`, which collide numerically and mean
entirely different things.

## Gotchas

- **Empty model output is expected on a small sample, not a bug.** Both models are gated on
  history: Layer 1's Isolation Forest needs `MIN_STATION_DAYS` (14) station-days for some
  location, Layer 2's ensemble needs `MIN_EVENT_ROWS` (20) feature rows. Below that the stage
  runs rules-only and says so in its log line. `data/bronze` currently holds a full month for
  four stations, so both models do train — a few files would not.
- **Rule R6 (`freshness_anomaly`) has been removed**, deliberately. Lateness is measured from
  the bronze manifest's `arrived_at`, i.e. when *we downloaded* the file, against OpenAQ's 72 h
  publication deadline. Backfilling January in September made every file ~240 days "late", so
  R6 fired on 90 of 98 station-days and dragged 160 of 164 alerts into quarantine. The
  `file_lateness_hours` metric is still computed and still feeds the Isolation Forest; only the
  rule is gone. Restore it behind a scheduled run, where the measurement means something.
  Rule ids are not renumbered — R6 is simply absent, with a comment in `rules.py` saying why.
- **Rule R7 (`missing_file`) assumes the archive filename convention.** A bronze zone holding
  only flat API exports (`openaq_location_*.csv`) has no file named for any station-day, so R7
  fires everywhere and every alert quarantines. Local-day spillover (Sydney is UTC+11) also
  makes it fire at the edges of a short date range.
- **Package `__init__.py` files are one-line docstrings.** They used to re-export every
  public name; nothing imported them that way, so the blocks were removed. Import from the
  submodule (`from pipelines.quality.metrics import ...`).
- **Ruff covers the complete pipeline, including `pipelines/detection/`.** Do not restore the
  old detection exclusion in CI.
- **Trained models are written to `models/` and gitignored** (`*.joblib`). `models/` also holds
  the version-controlled model cards, so do not clean the directory wholesale.
- **The bronze manifest is not safe for concurrent writers** — S3 has no append, so each
  arrival rewrites the object.

## Testing conventions

- The S3 branch is tested by monkeypatching `storage._wr` with a fake awswrangler, so the suite
  runs with no credentials. Never write tests that need real AWS.
- Pass roots as both `Path` and `str` where a stage accepts them; the CLI uses strings.
- `tests/test_pipeline_end_to_end.py` guards the wiring *between* stages (a real gzipped archive
  file with the `location_id` spelling must reach gold), not the detection logic inside them.

## Other agent configs

A Codex config exists at `~/.codex/config.toml`. Reply `/import` to scan and list what is
importable (MCP servers, slash commands, subagents, skills, instructions), then
`/import --yes=<digest>` (the scan output names the digest) to apply the user-level items. If
`/import` is not available on this surface, run `Codex import` from a terminal instead.
