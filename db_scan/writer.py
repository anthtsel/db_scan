import logging
import re
from pathlib import Path

from db_scan.scanner import ColumnInfo, ScanResult, TableSchema

logger = logging.getLogger(__name__)

_TAG_START = "<!-- DB_SCAN_START -->"
_TAG_END = "<!-- DB_SCAN_END -->"
_TAG_PATTERN = re.compile(
    r"<!-- DB_SCAN_START -->.*?<!-- DB_SCAN_END -->",
    re.DOTALL,
)

_CELL_MAX_LEN = 80


def render_markdown(results: ScanResult | list[ScanResult]) -> str:
    """
    Convert one or more ScanResults into a full Markdown string.

    Accepts a single ScanResult or a list (multi-source / directory scan).
    Each source gets its own H1 heading; tables are H2 within that section.
    """
    if isinstance(results, ScanResult):
        results = [results]

    sections: list[str] = []
    for result in results:
        sections.append(f"# Database Schema: `{result.source}`\n")
        for table in result.tables:
            sections.append(_render_table_section(table))

    return "\n".join(sections)


def _render_table_section(table: TableSchema) -> str:
    """Render one TableSchema as a Markdown block."""
    lines: list[str] = [
        f"## Table: `{table.name}`\n",
        f"- **Source type:** {table.source_type}",
        f"- **Row count:** {table.row_count:,}",
        f"- **Column count:** {len(table.columns)}",
        "",
        "### Columns",
        "",
        _render_columns_table(table.columns),
        "",
        "### Sample Data",
        "",
        _render_sample_rows(table.columns, table.sample_rows),
        "",
    ]
    return "\n".join(lines)


def _render_columns_table(columns: list[ColumnInfo]) -> str:
    """Render column definitions as a GFM Markdown table."""
    header = "| Name | Type | PK | Nullable | FK |"
    separator = "|------|------|----|----------|----|"
    rows = [header, separator]
    for col in columns:
        pk = "yes" if col.is_primary_key else "no"
        nullable = "yes" if col.is_nullable else "no"
        fk = _escape_cell(col.foreign_key_to) if col.foreign_key_to else "-"
        rows.append(
            f"| {_escape_cell(col.name)} | {_escape_cell(col.data_type)} "
            f"| {pk} | {nullable} | {fk} |"
        )
    return "\n".join(rows)


def _render_sample_rows(columns: list[ColumnInfo], rows: list[dict]) -> str:
    """Render sample data rows as a GFM Markdown table."""
    if not rows:
        return "_No rows available._"

    col_names = [col.name for col in columns]
    header = "| " + " | ".join(_escape_cell(c) for c in col_names) + " |"
    separator = "| " + " | ".join("---" for _ in col_names) + " |"

    table_rows = [header, separator]
    for row in rows:
        cells = []
        for col_name in col_names:
            value = row.get(col_name)
            if value is None or (isinstance(value, float) and value != value):
                # None or NaN
                cell = "NULL"
            else:
                cell = str(value)
                # Truncate long values
                if len(cell) > _CELL_MAX_LEN:
                    cell = cell[:_CELL_MAX_LEN] + "…"
            cells.append(_escape_cell(cell))
        table_rows.append("| " + " | ".join(cells) + " |")

    return "\n".join(table_rows)


def _escape_cell(value: str) -> str:
    """Escape Markdown table cell special characters."""
    # Replace newlines with a space so the table stays single-line
    value = value.replace("\n", " ").replace("\r", " ")
    # Escape pipe characters
    value = value.replace("|", r"\|")
    return value


def write_output(markdown: str, output_file: Path | None) -> None:
    """
    Write Markdown to a file or stdout.

    Raises:
        IOError: File write failure.
    """
    if output_file is None:
        print(markdown)
        return

    output_file.write_text(markdown, encoding="utf-8")
    logger.debug("Markdown written to %s", output_file)


def safe_write_claude(markdown: str, claude_path: Path) -> None:
    """
    Safely insert or update the schema block in a CLAUDE.md file.

    Searches for <!-- DB_SCAN_START --> / <!-- DB_SCAN_END --> tags:
    - If found: replaces only the content between them (tags preserved).
    - If not found: appends the block at the end of the file.

    Uses an atomic write (tmp file + rename) to prevent partial writes.

    Raises:
        ValueError: Mismatched START/END tags detected in the existing file.
        IOError: Read or write failure.
        FileNotFoundError: Parent directory of claude_path does not exist.
    """
    existing = claude_path.read_text(encoding="utf-8") if claude_path.exists() else ""

    # Pre-flight: catch manually broken tag pairs before we corrupt the file
    start_count = existing.count(_TAG_START)
    end_count = existing.count(_TAG_END)
    if start_count != end_count:
        raise ValueError(
            f"Mismatched DB_SCAN tags in '{claude_path}' "
            f"({start_count} START tag(s), {end_count} END tag(s)). "
            "Fix manually before re-running."
        )

    block = f"{_TAG_START}\n{markdown}\n{_TAG_END}"

    if _TAG_PATTERN.search(existing):
        updated = _TAG_PATTERN.sub(block, existing)
        logger.debug("Replaced existing DB_SCAN block in %s", claude_path)
    else:
        # Ensure there's a blank line separator before appending
        if existing and not existing.endswith("\n\n"):
            separator = "\n" if existing.endswith("\n") else "\n\n"
        else:
            separator = ""
        updated = existing + separator + block + "\n"
        logger.debug("Appended DB_SCAN block to %s", claude_path)

    # Atomic write: write to .tmp then rename
    tmp = claude_path.with_suffix(".tmp")
    tmp.write_text(updated, encoding="utf-8")
    tmp.replace(claude_path)
