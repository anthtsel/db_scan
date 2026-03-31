import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_primary_key: bool = False
    is_nullable: bool = True
    foreign_key_to: str | None = None  # "other_table.column" or None


@dataclass
class TableSchema:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int = 0
    sample_rows: list[dict] = field(default_factory=list)  # first 3 rows
    source_type: str = "database"  # "database" | "csv" | "xlsx"


@dataclass
class ScanResult:
    source: str
    tables: list[TableSchema] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def is_file_source(source: str) -> bool:
    """Return True if source is a supported data file path (based on _FILE_STRATEGY keys)."""
    return Path(source).suffix.lower() in _FILE_STRATEGY


def scan(source: str) -> ScanResult | list[ScanResult]:
    """
    Entry point. Dispatch based on source type:
    - Directory path  → scan_directory() returning list[ScanResult]
    - Supported file  → _scan_file() returning ScanResult
    - SQLAlchemy URL  → _scan_database() returning ScanResult

    Raises:
        ValueError: Unsupported format or bare SQLite file path without sqlite:// URL scheme.
        FileNotFoundError: File source path does not exist.
    """
    path = Path(source)

    if path.is_dir():
        return scan_directory(path)

    suffix = path.suffix.lower()

    if suffix in _SQLITE_SUFFIXES:
        raise ValueError(
            f"'{source}' looks like a SQLite file path. "
            f"Use a SQLAlchemy URL instead, e.g.: sqlite:///{source}"
        )

    if is_file_source(source):
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        return _scan_file(path)

    return _scan_database(source)


def scan_directory(directory: Path) -> list[ScanResult]:
    """
    Recursively walk a directory and scan all supported data files.

    Skips hidden paths (any component starting with '.').
    Returns one ScanResult per file found, sorted by path.

    Raises:
        ValueError: If no supported files are found in the directory.
    """
    results: list[ScanResult] = []

    for file_path in sorted(directory.rglob("*")):
        # Skip hidden directories and files (e.g. .git, .venv, .DS_Store)
        if any(part.startswith(".") for part in file_path.parts):
            continue
        if file_path.is_file() and file_path.suffix.lower() in _FILE_STRATEGY:
            logger.debug("Directory scan found: %s", file_path)
            results.append(_scan_file(file_path))

    if not results:
        supported = ", ".join(_FILE_STRATEGY)
        raise ValueError(
            f"No supported files found in '{directory}'. "
            f"Supported extensions: {supported}"
        )

    return results


def _scan_database(url: str) -> ScanResult:
    """
    Create a SQLAlchemy engine from url, reflect all tables, return ScanResult.

    Raises:
        sqlalchemy.exc.OperationalError: Connection or query failure.
    """
    engine = create_engine(url)
    result = ScanResult(source=url)

    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    if not table_names:
        result.warnings.append("No tables found in the database.")
        return result

    for table_name in table_names:
        logger.debug("Reflecting table: %s", table_name)
        table_schema = _reflect_table(engine, table_name)
        result.tables.append(table_schema)

    engine.dispose()
    return result


def _reflect_table(engine: Engine, table_name: str) -> TableSchema:
    """
    Reflect one table using SQLAlchemy inspector and fetch row count + sample rows.
    """
    inspector = inspect(engine)

    # Collect primary key column names
    pk_constraint = inspector.get_pk_constraint(table_name)
    pk_columns: set[str] = set(pk_constraint.get("constrained_columns", []))

    # Build FK lookup: column_name -> "referred_table.referred_column"
    fk_lookup: dict[str, str] = {}
    for fk in inspector.get_foreign_keys(table_name):
        for local_col, ref_col in zip(
            fk.get("constrained_columns", []),
            fk.get("referred_columns", []),
        ):
            fk_lookup[local_col] = f"{fk['referred_table']}.{ref_col}"

    # Build column list
    columns: list[ColumnInfo] = []
    for col in inspector.get_columns(table_name):
        col_name: str = col["name"]
        columns.append(
            ColumnInfo(
                name=col_name,
                data_type=str(col["type"]),
                is_primary_key=col_name in pk_columns,
                is_nullable=bool(col.get("nullable", True)),
                foreign_key_to=fk_lookup.get(col_name),
            )
        )

    # Fetch row count and sample rows
    quoted = f'"{table_name}"'
    row_count = 0
    sample_rows: list[dict] = []

    with engine.connect() as conn:
        try:
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM {quoted}"))
            row_count = count_result.scalar() or 0
        except Exception as exc:
            logger.warning("Could not count rows for table '%s': %s", table_name, exc)

        try:
            sample_result = conn.execute(
                text(f"SELECT * FROM {quoted} LIMIT 3")
            )
            sample_rows = [dict(row._mapping) for row in sample_result]
        except Exception as exc:
            logger.warning("Could not fetch sample rows for table '%s': %s", table_name, exc)

    table = TableSchema(
        name=table_name,
        columns=columns,
        row_count=row_count,
        sample_rows=sample_rows,
        source_type="database",
    )

    if row_count == 0:
        logger.debug("Table '%s' has 0 rows.", table_name)

    return table


def _scan_file(path: Path) -> ScanResult:
    """Dispatch to the appropriate file handler via _FILE_STRATEGY."""
    suffix = path.suffix.lower()
    handler = _FILE_STRATEGY.get(suffix)
    if handler is None:
        supported = ", ".join(_FILE_STRATEGY)
        raise ValueError(
            f"Unsupported file type '{suffix}'. Supported: {supported}"
        )
    return handler(path)


def _scan_csv(path: Path) -> ScanResult:
    """Load a CSV file and return a ScanResult with a single TableSchema."""
    result = ScanResult(source=str(path))

    df = pd.read_csv(path)

    # Warn if column names look like positional integers (no header row)
    if all(isinstance(c, int) for c in df.columns):
        result.warnings.append(
            f"CSV '{path.name}' appears to have no header row — "
            "column names are positional integers."
        )

    table = _dataframe_to_table_schema(df, name=path.stem, source_type="csv")
    result.tables.append(table)
    return result


def _scan_xlsx(path: Path) -> ScanResult:
    """Load an Excel file and return a ScanResult with one TableSchema per sheet."""
    result = ScanResult(source=str(path))

    xl = pd.ExcelFile(path)
    for sheet_name in xl.sheet_names:
        logger.debug("Reading sheet: %s", sheet_name)
        df = pd.read_excel(xl, sheet_name=sheet_name)

        if df.empty:
            result.warnings.append(
                f"Sheet '{sheet_name}' in '{path.name}' is empty."
            )

        table = _dataframe_to_table_schema(df, name=str(sheet_name), source_type="xlsx")
        result.tables.append(table)

    if not result.tables:
        result.warnings.append(f"No sheets found in '{path.name}'.")

    return result


def _dataframe_to_table_schema(
    df: pd.DataFrame, name: str, source_type: str
) -> TableSchema:
    """Convert a loaded DataFrame into a TableSchema. No PK/FK for file sources."""
    columns: list[ColumnInfo] = [
        ColumnInfo(
            name=str(col),
            data_type=_pandas_dtype_str(df[col].dtype),
        )
        for col in df.columns
    ]

    sample_df = df.head(3)
    sample_rows: list[dict] = [
        {str(col): row[col] for col in df.columns}
        for _, row in sample_df.iterrows()
    ]

    return TableSchema(
        name=name,
        columns=columns,
        row_count=len(df),
        sample_rows=sample_rows,
        source_type=source_type,
    )


def _pandas_dtype_str(dtype: object) -> str:
    """Convert a pandas/numpy dtype to a human-readable type name."""
    dtype_str = str(dtype)
    if dtype_str.startswith("int"):
        return "integer"
    if dtype_str.startswith("float"):
        return "float"
    if dtype_str.startswith("datetime"):
        return "datetime"
    if dtype_str == "bool":
        return "boolean"
    if dtype_str == "object":
        return "text"
    # Fallback: return the dtype string as-is (e.g. category, timedelta)
    return dtype_str


# Strategy map: file extension → scan function.
# Add new formats here without touching any other code.
_FILE_STRATEGY: dict[str, Callable[[Path], ScanResult]] = {
    ".csv":  _scan_csv,
    ".xlsx": _scan_xlsx,
}
