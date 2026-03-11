# /// script
# dependencies = ["duckdb>=0.10.0", "rich>=13.0.0"]
# requires-python = ">=3.8"
# ///

import sys
import duckdb
import os
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

console = Console()


def get_file_extension(filepath):
    return os.path.splitext(filepath)[1].lower()


def load_file(conn, filepath):
    ext = get_file_extension(filepath)

    # Drop existing table if exists
    conn.execute("DROP TABLE IF EXISTS data")

    if ext == ".csv":
        conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{filepath}')")
    elif ext == ".parquet":
        conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{filepath}')")
    else:
        raise ValueError(f"Unsupported file type: {ext}. Use .csv or .parquet")


def get_summary(conn):
    # Get row count
    row_count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]

    # Get column info - DESCRIBE returns (column_name, column_type, null, key, default, extra)
    schema = conn.execute("DESCRIBE data").fetchall()

    columns = []
    for col_info in schema:
        col_name = col_info[0]
        col_type = col_info[1]

        col_stats = {"name": col_name, "type": col_type, "null_count": 0}

        # Get null count
        null_count = conn.execute(
            f'''SELECT COUNT(*) - COUNT("{col_name}") FROM data'''
        ).fetchone()[0]
        col_stats["null_count"] = null_count

        type_lower = col_type.lower()

        if any(t in type_lower for t in ["int", "bigint", "smallint", "tinyint"]):
            # Integer column
            stats = conn.execute(f'''
                SELECT 
                    MIN("{col_name}"),
                    MAX("{col_name}"),
                    MEDIAN("{col_name}")
                FROM data
            ''').fetchone()
            col_stats["min"] = stats[0]
            col_stats["max"] = stats[1]
            col_stats["median"] = stats[2]

        elif any(t in type_lower for t in ["float", "double", "decimal", "numeric"]):
            # Floating point column
            stats = conn.execute(f'''
                SELECT 
                    MIN("{col_name}"),
                    MAX("{col_name}"),
                    MEDIAN("{col_name}"),
                    AVG("{col_name}"),
                    STDDEV("{col_name}")
                FROM data
            ''').fetchone()
            col_stats["min"] = stats[0]
            col_stats["max"] = stats[1]
            col_stats["median"] = stats[2]
            col_stats["mean"] = stats[3]
            col_stats["stddev"] = stats[4]

        elif any(t in type_lower for t in ["varchar", "char", "text", "string"]):
            # String column
            stats = conn.execute(f'''
                SELECT 
                    COUNT(DISTINCT "{col_name}"),
                    MIN(LENGTH("{col_name}")),
                    MAX(LENGTH("{col_name}"))
                FROM data
            ''').fetchone()
            col_stats["unique_count"] = stats[0]
            col_stats["min_length"] = stats[1]
            col_stats["max_length"] = stats[2]

        columns.append(col_stats)

    return {"row_count": row_count, "columns": columns}


def format_value(val):
    if val is None:
        return "N/A"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def print_summary(summary):
    # Header panel with row count
    header_text = f"[bold]Total Rows:[/bold] {summary['row_count']:,}"
    console.print(
        Panel(header_text, title="[bold]Summary Statistics[/bold]", expand=False)
    )

    for col in summary["columns"]:
        # Create a table for each column's stats
        table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
        table.add_column("Stat", style="dim", width=12)
        table.add_column("Value", width=20)

        # Add null count for all columns
        table.add_row("Nulls:", str(col["null_count"]))

        if "min" in col:
            table.add_row("Min:", format_value(col["min"]))
            table.add_row("Max:", format_value(col["max"]))
            table.add_row("Median:", format_value(col["median"]))

        if "mean" in col:
            table.add_row("Mean:", format_value(col["mean"]))
            table.add_row("Std Dev:", format_value(col["stddev"]))

        if "unique_count" in col:
            table.add_row("Unique:", f"{col['unique_count']:,}")
            table.add_row("Min Len:", str(col["min_length"]))
            table.add_row("Max Len:", str(col["max_length"]))

        # Column name bold, type in blue
        title = f"[bold]{col['name']}[/bold] [blue]({col['type']})[/blue]"
        console.print(Panel(table, title=title, expand=False))
        console.print()


def print_data(conn, where_clause=None, limit=10):
    # Build query
    sql = "SELECT * FROM data"
    if where_clause:
        sql += f" WHERE {where_clause}"
    sql += f" LIMIT {limit}"

    # Get column names
    schema = conn.execute("DESCRIBE data").fetchall()
    col_names = [col[0] for col in schema]

    # Get data
    result = conn.execute(sql).fetchall()

    # Create rich table
    title = "First 10 Rows"
    if where_clause:
        title += f"\n[dim](Filtered: WHERE {where_clause})[/dim]"

    table = Table(title=title, box=box.SIMPLE_HEAD, show_lines=False)

    # Add columns - bold headers
    for col_name in col_names:
        table.add_column(col_name, style="bold")

    if not result:
        console.print("\n[dim]No rows to display.[/dim]\n")
        return

    # Add rows
    for row in result:
        formatted_row = [format_cell(val) for val in row]
        table.add_row(*formatted_row)

    console.print(table)
    console.print()


def format_cell(val):
    if val is None:
        return "[dim]NULL[/dim]"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze.py <filepath> [where_clause]")
        print("\nExamples:")
        print("  python analyze.py data.csv")
        print('  python analyze.py data.parquet "age > 25"')
        print("  python analyze.py data.csv \"city = 'New York'\"")
        sys.exit(1)

    filepath = sys.argv[1]
    where_clause = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        sys.exit(1)

    # Connect to in-memory DuckDB
    conn = duckdb.connect(":memory:")

    try:
        # Load the file
        console.print(f"Loading: [blue]{filepath}[/blue]\n")
        load_file(conn, filepath)

        # Get and print summary
        summary = get_summary(conn)
        print_summary(summary)

        # Print first 10 rows
        print_data(conn, where_clause)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
