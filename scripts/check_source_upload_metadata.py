"""Check whether Synapse exposes source upload/ingestion dates.

This script is read-only. It looks for three signals:
1. Audit-like columns on the source tables, then prints MAX(column).
2. Object create/modify metadata from sys.objects.
3. Recent load-like requests in sys.dm_pdw_exec_requests, if permitted.
"""
from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import pandas as pd
import pyodbc

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.config import Config, DemandConfig, DiagnosisConfig  # noqa: E402


AUDIT_NAME_PATTERNS = (
    "load",
    "upload",
    "ingest",
    "import",
    "etl",
    "extract",
    "refresh",
    "update",
    "modified",
    "created",
    "audit",
)


SOURCE_TABLES = (
    ("demand", DemandConfig.SCHEMA, DemandConfig.TABLE_NAME),
    ("diagnosis", DiagnosisConfig.SCHEMA, DiagnosisConfig.TABLE_NAME),
)

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable.*",
    category=UserWarning,
)


def print_section(title: str) -> None:
    print(f"\n=== {title} ===")


def print_frame(df: pd.DataFrame, empty_message: str) -> None:
    if df.empty:
        print(empty_message)
        return
    print(df.to_string(index=False))


def read_sql(conn, query: str, params: list | tuple | None = None) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=list(params or []))


def get_connection(
    db_server: str,
    db_database: str,
    auth_mode: str,
    driver: str,
    timeout: int = 60,
) -> pyodbc.Connection:
    connection_string = (
        f"Driver={{{driver}}};"
        f"Server={db_server};"
        "Port=1433;"
        f"Database={db_database};"
        f"Authentication={auth_mode};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    return pyodbc.connect(connection_string, timeout=timeout)


def get_object_metadata(conn) -> pd.DataFrame:
    table_filters = " OR ".join(
        ["(LOWER(t.TABLE_SCHEMA) = LOWER(?) AND LOWER(t.TABLE_NAME) = LOWER(?))"]
        * len(SOURCE_TABLES)
    )
    params: list[str] = []
    for _, schema, table in SOURCE_TABLES:
        params.extend([schema, table])

    query = f"""
    SELECT
        t.TABLE_SCHEMA AS schema_name,
        t.TABLE_NAME AS table_name,
        t.TABLE_TYPE AS table_type,
        o.create_date,
        o.modify_date
    FROM INFORMATION_SCHEMA.TABLES t
    LEFT JOIN sys.schemas s
        ON LOWER(s.name) = LOWER(t.TABLE_SCHEMA)
    LEFT JOIN sys.objects o
        ON o.schema_id = s.schema_id
       AND LOWER(o.name) = LOWER(t.TABLE_NAME)
    WHERE ({table_filters})
    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
    """
    return read_sql(conn, query, params)


def get_direct_table_access(conn) -> pd.DataFrame:
    rows = []
    for dataset, schema, table in SOURCE_TABLES:
        query = f"SELECT TOP 1 1 AS can_read FROM [{schema}].[{table}];"
        try:
            read_sql(conn, query)
            rows.append(
                {
                    "dataset": dataset,
                    "schema_name": schema,
                    "table_name": table,
                    "can_read": "yes",
                    "message": "direct SELECT works",
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "dataset": dataset,
                    "schema_name": schema,
                    "table_name": table,
                    "can_read": "no",
                    "message": str(exc),
                }
            )

    return pd.DataFrame(rows)


def get_related_tables(conn) -> pd.DataFrame:
    schema_names = sorted({schema for _, schema, _ in SOURCE_TABLES})
    table_terms = sorted(
        {
            "P1038",
            *[
                part
                for _, _, table in SOURCE_TABLES
                for part in table.replace("-", "_").split("_")
                if len(part) >= 5
            ],
        }
    )
    schema_filters = " OR ".join(["LOWER(TABLE_SCHEMA) = LOWER(?)"] * len(schema_names))
    table_filters = " OR ".join(["LOWER(TABLE_NAME) LIKE LOWER(?)"] * len(table_terms))
    params = [*schema_names, *[f"%{term}%" for term in table_terms]]

    query = f"""
    SELECT
        TABLE_SCHEMA AS schema_name,
        TABLE_NAME AS table_name,
        TABLE_TYPE AS table_type
    FROM INFORMATION_SCHEMA.TABLES
    WHERE ({schema_filters})
      AND ({table_filters})
    ORDER BY TABLE_SCHEMA, TABLE_NAME;
    """
    return read_sql(conn, query, params)


def get_audit_like_columns(conn) -> pd.DataFrame:
    pattern_filters = " OR ".join(["LOWER(COLUMN_NAME) LIKE ?"] * len(AUDIT_NAME_PATTERNS))
    table_filters = " OR ".join(
        ["(LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?))"]
        * len(SOURCE_TABLES)
    )

    params: list[str] = [f"%{pattern}%" for pattern in AUDIT_NAME_PATTERNS]
    for _, schema, table in SOURCE_TABLES:
        params.extend([schema, table])

    query = f"""
    SELECT
        TABLE_SCHEMA AS schema_name,
        TABLE_NAME AS table_name,
        COLUMN_NAME AS column_name,
        DATA_TYPE AS data_type
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE ({pattern_filters})
      AND ({table_filters})
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
    """
    return read_sql(conn, query, params)


def get_candidate_max_values(conn, audit_columns: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in audit_columns.itertuples(index=False):
        schema = row.schema_name
        table = row.table_name
        column = row.column_name
        query = f"""
        SELECT MAX(TRY_CAST([{column}] AS datetime2)) AS max_upload_candidate
        FROM [{schema}].[{table}]
        WHERE [{column}] IS NOT NULL;
        """
        try:
            max_df = read_sql(conn, query)
            max_value = max_df.loc[0, "max_upload_candidate"] if not max_df.empty else None
            status = "ok"
        except Exception as exc:
            max_value = None
            status = f"could not cast to datetime: {exc}"

        rows.append(
            {
                "schema_name": schema,
                "table_name": table,
                "column_name": column,
                "data_type": row.data_type,
                "max_upload_candidate": max_value,
                "status": status,
            }
        )

    return pd.DataFrame(rows)


def get_recent_load_requests(conn) -> pd.DataFrame:
    table_patterns = [f"%{table}%" for _, _, table in SOURCE_TABLES]
    command_filters = " OR ".join(["[command] LIKE ?"] * len(table_patterns))
    load_filters = """
        [command] LIKE '%COPY%'
        OR [command] LIKE '%INSERT%'
        OR [command] LIKE '%CTAS%'
        OR [command] LIKE '%CREATE TABLE AS%'
    """
    query = f"""
    SELECT TOP 50
        request_id,
        [status],
        submit_time,
        start_time,
        end_time,
        [label],
        SUBSTRING([command], 1, 500) AS command_preview
    FROM sys.dm_pdw_exec_requests
    WHERE ({command_filters})
      AND ({load_filters})
    ORDER BY end_time DESC, start_time DESC, submit_time DESC;
    """
    return read_sql(conn, query, table_patterns)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check source tables for upload/ingestion date metadata."
    )
    parser.add_argument("--server", default=Config.DB_SERVER)
    parser.add_argument("--database", default=Config.DB_DATABASE)
    parser.add_argument("--auth-mode", default=Config.AUTH_MODE)
    parser.add_argument("--driver", default="ODBC Driver 18 for SQL Server")
    parser.add_argument("--skip-dmv", action="store_true", help="Skip sys.dm_pdw_exec_requests.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print(
        f"Connecting to {args.server} / {args.database} using "
        f"{args.auth_mode} and {args.driver}..."
    )
    try:
        conn = get_connection(
            args.server,
            args.database,
            auth_mode=args.auth_mode,
            driver=args.driver,
        )
    except pyodbc.Error as exc:
        print(f"Could not connect: {exc}")
        print("Installed ODBC drivers:")
        drivers = pyodbc.drivers()
        if drivers:
            for driver in drivers:
                print(f"  - {driver}")
        else:
            print("  (none reported by pyodbc)")
        print(
            "Install 'ODBC Driver 18 for SQL Server' or rerun with "
            "--driver \"<installed driver name>\"."
        )
        return 1

    try:
        print_section("Direct Table Access")
        access = get_direct_table_access(conn)
        print_frame(access, "Could not check direct table access.")

        print_section("Table Metadata")
        metadata = get_object_metadata(conn)
        print_frame(
            metadata,
            "No table metadata found. Check schema/table names and permissions.",
        )
        print("Note: modify_date is object/index metadata, not a reliable data upload date.")

        if metadata.empty:
            print_section("Related Tables In Configured Schema")
            related = get_related_tables(conn)
            print_frame(
                related,
                "No related P1038-like tables found in the configured schema.",
            )

        print_section("Audit-Like Columns")
        audit_columns = get_audit_like_columns(conn)
        print_frame(audit_columns, "No audit-like columns found on these source tables.")

        if not audit_columns.empty:
            print_section("Max Audit-Like Column Values")
            max_values = get_candidate_max_values(conn, audit_columns)
            print_frame(max_values, "No candidate upload values found.")

        if not args.skip_dmv:
            print_section("Recent Load-Like Requests")
            try:
                requests = get_recent_load_requests(conn)
                print_frame(
                    requests,
                    "No recent load-like requests found in Synapse DMV history.",
                )
            except Exception as exc:
                print(f"Could not read sys.dm_pdw_exec_requests: {exc}")
                print("You may need VIEW DATABASE STATE permission, or the load is no longer in DMV history.")

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
