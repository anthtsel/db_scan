# DB-Scan

Inspect databases and data files, then generate a Markdown schema summary optimised for dropping into LLM context windows.

## Features

- **Database support** — SQLite out of the box; Postgres and MySQL via optional extras
- **File support** — CSV and Excel (`.xlsx`, per-sheet) via Pandas
- **Directory scanning** — auto-detects all supported files recursively
- **Multi-source** — scan multiple databases/files in one command, or point at a YAML config
- **LLM-ready output** — Markdown with column names, types, PK/FK constraints, and 3-row samples per table
- **CLAUDE.md safe-write** — injects/updates the schema block between `<!-- DB_SCAN_START -->` tags without touching the rest of the file

## Installation

Requires Python 3.10+. Using [uv](https://github.com/astral-sh/uv) (recommended):

```bash
git clone https://github.com/anthtsel/db_scan.git
cd db_scan
uv venv && source .venv/bin/activate
uv pip install -e .
```

With pip:

```bash
pip install -e .
```

### Optional extras

```bash
# PostgreSQL
uv pip install -e ".[postgres]"

# MySQL
uv pip install -e ".[mysql]"
```

## Usage

```
db-scan [SOURCES]... [OPTIONS]
```

### Scan a SQLite database

```bash
db-scan scan sqlite:///path/to/app.db
```

### Scan a CSV or Excel file

```bash
db-scan scan data.csv
db-scan scan reports.xlsx
```

### Scan a directory (auto-detects .csv and .xlsx)

```bash
db-scan scan ./exports/
```

### Scan multiple sources at once

```bash
db-scan scan sqlite:///app.db data.csv ./exports/
```

### Write output to a file

```bash
db-scan scan sqlite:///app.db --output schema.md
```

### Update CLAUDE.md with the schema

```bash
db-scan scan sqlite:///app.db --write-claude CLAUDE.md
```

The schema is inserted between `<!-- DB_SCAN_START -->` and `<!-- DB_SCAN_END -->` tags. Running the command again replaces only that block — the rest of `CLAUDE.md` is untouched.

### Use a config file

```bash
db-scan scan --config sources.yaml
```

`sources.yaml` format:

```yaml
sources:
  - sqlite:///path/to/app.db
  - postgresql://user:pass@localhost/analytics
  - ./data/exports.csv
  - ./reports/
```

Sources from `--config` and positional arguments can be combined.

## Output Example

```markdown
# Database Schema: `sqlite:///app.db`

## Table: `users`

- **Source type:** database
- **Row count:** 1,042
- **Column count:** 5

### Columns

| Name       | Type        | PK  | Nullable | FK       |
|------------|-------------|-----|----------|----------|
| id         | INTEGER     | yes | no       | -        |
| username   | VARCHAR(80) | no  | no       | -        |
| email      | VARCHAR(120)| no  | no       | -        |
| role_id    | INTEGER     | no  | yes      | roles.id |
| created_at | DATETIME    | no  | yes      | -        |

### Sample Data

| id | username | email             | role_id | created_at          |
|----|----------|-------------------|---------|---------------------|
| 1  | alice    | alice@example.com | 2       | 2024-01-15 09:32:00 |
| 2  | bob      | bob@example.com   | NULL    | 2024-01-16 11:00:00 |
| 3  | carol    | carol@example.com | 1       | 2024-01-17 14:22:00 |
```

## Tech Stack

| Package     | Role                          |
|-------------|-------------------------------|
| Typer       | CLI interface                 |
| Rich        | Terminal output & progress    |
| SQLAlchemy  | Database reflection           |
| Pandas      | CSV / Excel parsing           |
| Openpyxl    | Excel file engine             |
| PyYAML      | Config file parsing           |

## Project Structure

```
db_scan/
├── pyproject.toml
└── db_scan/
    ├── __init__.py   # package version
    ├── main.py       # CLI entry point, orchestration
    ├── scanner.py    # DB reflection + file scanning, returns ScanResult dataclasses
    └── writer.py     # Markdown rendering + CLAUDE.md safe-write
```

## Extending File Support

To add a new file format, add one entry to `_FILE_STRATEGY` in `scanner.py`:

```python
_FILE_STRATEGY: dict[str, Callable[[Path], ScanResult]] = {
    ".csv":     _scan_csv,
    ".xlsx":    _scan_xlsx,
    ".parquet": _scan_parquet,  # add your handler here
}
```

No other code needs to change.

## License

MIT
