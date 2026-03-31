import logging
from pathlib import Path
from typing import Optional

import typer
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from sqlalchemy.exc import OperationalError

from db_scan.scanner import ScanResult, scan
from db_scan.writer import render_markdown, safe_write_claude, write_output

app = typer.Typer(
    name="db-scan",
    help="Inspect databases and data files; generate Markdown schema for LLM context.",
    add_completion=False,
)

# Two consoles: one for stdout, one for stderr
console = Console()
err_console = Console(stderr=True)

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _load_config(config_path: Path) -> list[str]:
    """
    Load a YAML config file and return the list of source strings.

    Expected format:
        sources:
          - sqlite:///path/to/app.db
          - ./data/exports.csv
          - ./reports/

    Raises:
        ValueError: Missing 'sources' key or invalid format.
        FileNotFoundError: Config file does not exist.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or "sources" not in data:
        raise ValueError(
            f"Config file '{config_path}' must contain a top-level 'sources' list."
        )

    sources = data["sources"]
    if not isinstance(sources, list) or not sources:
        raise ValueError(
            f"'sources' in '{config_path}' must be a non-empty list of strings."
        )

    return [str(s) for s in sources]


def _scan_source(source: str) -> list[ScanResult]:
    """
    Scan one source and always return a list[ScanResult].
    Directories return multiple results; single DB/file returns one.
    """
    result = scan(source)
    if isinstance(result, list):
        return result
    return [result]


@app.command()
def scan_cmd(
    sources: Optional[list[str]] = typer.Argument(
        default=None,
        help=(
            "One or more SQLAlchemy URLs, file paths (.csv/.xlsx), or directory paths. "
            "Can be combined with --config."
        ),
    ),
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help=(
            "Path to a YAML config file listing sources. "
            "Format: a top-level 'sources' key with a list of URLs/paths."
        ),
        exists=False,  # we check manually for a better error message
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Write Markdown output to FILE instead of stdout.",
        writable=True,
    ),
    write_claude: Optional[Path] = typer.Option(
        None,
        "--write-claude",
        help=(
            "Path to a CLAUDE.md file to update with the schema. "
            "Content is inserted between <!-- DB_SCAN_START --> and <!-- DB_SCAN_END --> tags. "
            "Do not point this at the same file as --output."
        ),
    ),
) -> None:
    """Scan one or more databases or data files and emit a Markdown schema summary."""

    # --- Resolve all sources ---
    all_sources: list[str] = []

    if config:
        try:
            all_sources.extend(_load_config(config))
        except (FileNotFoundError, ValueError) as exc:
            err_console.print(f"[bold red]Error:[/bold red] {exc}")
            raise typer.Exit(1)

    if sources:
        all_sources.extend(sources)

    if not all_sources:
        err_console.print(
            "[bold red]Error:[/bold red] Provide at least one source argument or --config."
        )
        raise typer.Exit(1)

    # --- Scan all sources with a progress spinner ---
    all_results: list[ScanResult] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=err_console,
        transient=True,
    ) as progress:
        for source in all_sources:
            task = progress.add_task(
                description=f"Scanning [bold]{source}[/bold]...", total=None
            )
            try:
                results = _scan_source(source)
                all_results.extend(results)
            except FileNotFoundError as exc:
                err_console.print(f"[bold red]Error:[/bold red] {exc}")
                raise typer.Exit(1)
            except OperationalError as exc:
                err_console.print(
                    f"[bold red]Error:[/bold red] Database connection failed.\n{exc.orig}"
                )
                raise typer.Exit(1)
            except ValueError as exc:
                err_console.print(f"[bold red]Error:[/bold red] {exc}")
                raise typer.Exit(1)
            finally:
                progress.remove_task(task)

    # --- Surface non-fatal warnings from all results ---
    for result in all_results:
        for warning in result.warnings:
            err_console.print(f"[yellow]Warning:[/yellow] [{result.source}] {warning}")

    # --- Rich summary table ---
    # When Markdown goes to stdout (output=None), print summary to stderr to keep piping clean.
    summary_console = err_console if output is None else console

    rich_table = Table(
        title="Scan Summary",
        show_header=True,
        header_style="bold cyan",
        show_lines=False,
    )
    rich_table.add_column("Source", style="dim", no_wrap=True)
    rich_table.add_column("Table / Sheet", style="bold white")
    rich_table.add_column("Type", style="dim")
    rich_table.add_column("Rows", justify="right", style="green")
    rich_table.add_column("Columns", justify="right", style="blue")

    for result in all_results:
        for t in result.tables:
            rich_table.add_row(
                result.source,
                t.name,
                t.source_type,
                f"{t.row_count:,}",
                str(len(t.columns)),
            )

    summary_console.print(rich_table)

    # --- Render and write Markdown ---
    markdown = render_markdown(all_results)

    try:
        write_output(markdown, output)
    except IOError as exc:
        err_console.print(f"[bold red]Error:[/bold red] Could not write output file: {exc}")
        raise typer.Exit(1)

    if output:
        console.print(f"[green]Markdown written to:[/green] {output}")

    # --- Safe-write to CLAUDE.md ---
    if write_claude:
        try:
            safe_write_claude(markdown, write_claude)
            console.print(f"[green]CLAUDE.md updated:[/green] {write_claude}")
        except ValueError as exc:
            err_console.print(f"[bold red]Error:[/bold red] {exc}")
            raise typer.Exit(1)
        except (IOError, FileNotFoundError) as exc:
            err_console.print(
                f"[bold red]Error:[/bold red] Could not update CLAUDE.md: {exc}"
            )
            raise typer.Exit(1)


if __name__ == "__main__":
    app()
