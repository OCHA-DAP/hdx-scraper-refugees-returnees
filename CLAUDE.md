# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-refugees-returnees** pulls together global refugee and returnee data for use in HAPI. It reads a
single UNHCR source dataset from HDX (the "Demographics and locations of forcibly displaced and stateless people
(Global)" dataset) and creates two HAPI datasets in HDX: a refugees dataset (split into multiple CSV resources by
5-year date range) and a single returnees CSV.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.refugees_returnees
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_pipeline.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py` and `pipeline.py`:

1. **`main`** (`__main__.py`) — reads configuration, checks HDX write access, downloads the source CSV via
   `Retrieve`, runs the `Pipeline` to build the refugees/returnees data, then generates and creates each output
   dataset in HDX.
2. **`Pipeline.get_data`** — downloads the UNHCR source CSV, sums population columns grouped by year/country of
   origin/country of asylum/population type, classifies each row into a refugee category (REF, ROC, ASY, OIP, IOC,
   STA, OOC, HST, RST, NAT) or returnee category (RET, RDP, RRI), extracts gender (`m`/`f`/`all`) and age range from
   population column headers, and enriches each row with HRP (`has_hrp`) and GHO (`in_gho`) status looked up per
   ISO3 code.
3. **`Pipeline.generate_dataset`** — builds the HDX `Dataset` object for `"refugees"` or `"returnees"`. The
   returnees dataset is a single CSV resource; the refugees dataset is split into multiple CSV resources, one per
   5-year date range (e.g. 2000-2004, 2005-2009, ...).

### Key design points

- **Config-driven**: `config/project_configuration.yaml` defines the source dataset/resource, output dataset
  names/titles, tags, resource templates, and output column headers. `config/hdx_dataset_static.yaml` holds static
  HDX dataset metadata applied via `update_from_yaml`.
- **Single API read, two writes**: one HDX read (the UNHCR source dataset) produces two HDX dataset writes
  (refugees and returnees).
- **`saved_data/`**: kept in the repo (not `/tmp`) so downloaded/generated data survives between runs when using
  `--save`/`--use-saved`.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`,
`EXTRA_PARAMS`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-refugees-returnees` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
