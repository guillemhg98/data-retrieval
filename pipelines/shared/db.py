"""Database connection utilities for SQL Server/Synapse."""
import os
import time

import pyodbc


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _build_connection_string(
    db_server: str,
    db_database: str,
    auth_mode: str,
) -> str:
    """Build an ODBC connection string from environment-aware settings."""
    raw_connection_string = os.getenv("DB_CONNECTION_STRING", "").strip()
    if raw_connection_string:
        return raw_connection_string

    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    port = os.getenv("DB_PORT", "1433")
    uid = os.getenv("DB_UID", "").strip()
    password = os.getenv("DB_PASSWORD", "")

    parts = [
        f"Driver={{{driver}}}",
        f"Server={db_server}",
        f"Port={port}",
        f"Database={db_database}",
        f"Authentication={auth_mode}",
        "Encrypt=yes",
        "TrustServerCertificate=no",
    ]
    if uid:
        parts.append(f"UID={uid}")
    if password:
        parts.append(f"PWD={password}")

    return ";".join(parts) + ";"


def _is_retryable_connection_error(exc: pyodbc.Error) -> bool:
    """Return True for transient connection problems worth retrying."""
    text = str(exc).lower()
    non_retryable = [
        "login failed",
        "token is expired",
        "invalid authorization",
        "password",
    ]
    if any(fragment in text for fragment in non_retryable):
        return False

    retryable = [
        "timeout",
        "temporarily unavailable",
        "connection may have been terminated",
        "communication link failure",
        "transport-level error",
    ]
    return any(fragment in text for fragment in retryable)


def get_connection(
    db_server: str,
    db_database: str,
    auth_mode: str = "ActiveDirectoryIntegrated",
    timeout: int = 60,
) -> pyodbc.Connection:
    """
    Create a connection to SQL Server or Azure Synapse.
    
    Args:
        db_server: Server address (e.g., 'server.sql.azuresynapse.net')
        db_database: Database name
        auth_mode: Authentication mode (default: ActiveDirectoryIntegrated)
        timeout: Connection timeout in seconds
        
    Returns:
        pyodbc.Connection: Database connection
    """
    connection_string = _build_connection_string(
        db_server=db_server,
        db_database=db_database,
        auth_mode=auth_mode,
    )
    retries = max(1, _env_int("DB_CONNECT_RETRIES", 1))
    retry_delay = max(0, _env_int("DB_CONNECT_RETRY_DELAY_SECONDS", 10))

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(connection_string, timeout=timeout)
        except pyodbc.Error as exc:
            last_error = exc
            if attempt >= retries or not _is_retryable_connection_error(exc):
                raise
            time.sleep(retry_delay)

    raise last_error
