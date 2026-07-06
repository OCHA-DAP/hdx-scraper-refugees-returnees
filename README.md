# Collector for Refugees and Returnees Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-refugees-returnees/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-refugees-returnees/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-refugees-returnees/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-refugees-returnees?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script pulls together global refugee and returnee data for use in HAPI,
reading a single UNHCR source dataset from HDX and creating two HAPI datasets.
It makes 1 HDX read call (to the UNHCR "Demographics and locations of forcibly
displaced and stateless people (Global)" dataset) and 2 HDX writes. The output
consists of a refugees dataset (split into multiple CSV resources by 5-year date
range, each up to a few MB) and a single returnees CSV (up to a few MB). The
source CSV is parsed to classify population types into 10 refugee categories
(REF, ROC, ASY, OIP, IOC, STA, OOC, HST, RST, NAT) and 3 returnee categories
(RET, RDP, RRI); gender (Male/Female/All) and age ranges are extracted from
column headers; records are grouped by year, country of origin, country of
asylum, and population type; and each output row is enriched with HRP and GHO
status.

## Data Pipeline

### API reads (~1 call per run)

- **UNHCR source dataset** (1 read): downloads the UNHCR "Demographics and
  locations of forcibly displaced and stateless people (Global)" CSV from HDX.

### API writes (~2 calls per run)

- **Refugees dataset** (1 write): multiple CSV resources partitioned by 5-year
  date range, each up to a few MB.
- **Returnees dataset** (1 write): a single CSV resource, up to a few MB.

### Temporary files

- None significant; data is processed in memory.

### Uploaded files

- Refugees dataset: multiple CSVs partitioned by 5-year date range (e.g.
  2000–2004, 2005–2009, …), each up to a few MB.
- Single returnees CSV, up to a few MB.

### Transformations

1. **Population type classification**: source population types are mapped to 10
   refugee categories (REF, ROC, ASY, OIP, IOC, STA, OOC, HST, RST, NAT) and 3
   returnee categories (RET, RDP, RRI).
2. **Gender and age extraction**: gender (Male/Female/All) and age range values
   are parsed from column headers.
3. **Grouping**: records are aggregated by year, country of origin, country of
   asylum, and population type.
4. **HRP/GHO enrichment**: each output row is annotated with `has_hrp` and
   `in_gho` flags looked up per ISO3 code.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-refugees_returnees** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.refugees_returnees
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
