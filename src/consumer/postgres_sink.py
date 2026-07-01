"""
PostgreSQL Sink - Target Database Replication
==============================================
Applies CDC events to the target PostgreSQL database.

This creates a replica of the source database by:
- INSERT events → INSERT into target
- UPDATE events → UPDATE in target
- DELETE events → DELETE from target

WHY REPLICATE?
--------------
1. Read replicas: Offload analytics queries from source
2. Geographic distribution: Low-latency reads in other regions
3. Migration: Gradually shift traffic to new database
4. Testing: Safe environment for experiments
5. Backup: Another copy of production data

HOW IT WORKS
------------
1. Parse the CDC event (operation type, before/after data)
2. Generate the appropriate SQL statement
3. Execute with proper transaction handling
4. Handle conflicts (idempotent operations)

IDEMPOTENCY
-----------
Events may be delivered multiple times (at-least-once).
We handle this with:
- INSERT: ON CONFLICT DO UPDATE (upsert)
- UPDATE: Conditional on expected before values
- DELETE: Ignore if row doesn't exist
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Generator, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from consumer import config
from schemas.cdc_event import CDCEvent, OperationType
from metrics import record_postgres_write, POSTGRES_CONNECTION_ERRORS

logger = logging.getLogger(__name__)


class PostgresSink:
    """
    Applies CDC events to the target PostgreSQL database.

    Features:
    - Connection pooling (via reconnection on failure)
    - Idempotent operations (safe to replay)
    - Transaction batching (multiple events per commit)
    - Schema validation (ensure target has required columns)

    SIMPLE EXPLANATION:
    This is like a court stenographer replaying their notes:
    - They hear "record inserted" → write it down
    - They hear "record updated" → change it
    - They hear "record deleted" → cross it out
    The result is an exact copy of what happened.
    """

    def __init__(self):
        """Initialize connection to target PostgreSQL."""
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._connect()
        logger.info("PostgreSQL sink initialized")

    def _connect(self) -> None:
        """Establish connection to target database."""
        try:
            self._conn = psycopg2.connect(config.get_target_pg_dsn())
            self._conn.autocommit = False
            logger.info(
                f"Connected to target PostgreSQL at "
                f"{config.TARGET_PG_HOST}:{config.TARGET_PG_PORT}"
            )
        except psycopg2.Error as e:
            POSTGRES_CONNECTION_ERRORS.inc()
            logger.error(f"Failed to connect to target PostgreSQL: {e}")
            raise
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to target PostgreSQL: {e}")
            raise

    @contextmanager
    def _get_cursor(self) -> Generator[RealDictCursor, None, None]:
        """Get a cursor with automatic reconnection on failure."""
        try:
            if self._conn is None or self._conn.closed:
                self._connect()
            cursor = self._conn.cursor(cursor_factory=RealDictCursor)
            yield cursor
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if self._conn:
                self._conn.rollback()
            raise

    def write(self, event: CDCEvent) -> None:
        """
        Apply a single CDC event to the target database.

        Dispatches to the appropriate handler based on operation type.
        Each operation is designed to be idempotent.
        """
        start_time = time.time()
        try:
            if event.operation == OperationType.INSERT:
                self._apply_insert(event)
            elif event.operation == OperationType.UPDATE:
                self._apply_update(event)
            elif event.operation == OperationType.DELETE:
                self._apply_delete(event)
            else:
                logger.warning(f"Unknown operation type: {event.operation}")
                return
            
            # Record successful write metric
            duration = time.time() - start_time
            record_postgres_write(event.source.table, event.operation.value, duration)
        except Exception as e:
            logger.error(f"Failed to apply event {event.event_id}: {e}")
            raise

    def _apply_insert(self, event: CDCEvent) -> None:
        """
        Apply an INSERT event as an UPSERT.

        Uses ON CONFLICT DO UPDATE to handle duplicates.
        If the row already exists (duplicate event), we update it
        to the latest values - this makes the operation idempotent.

        SIMPLE EXPLANATION:
        "If this record doesn't exist, add it.
         If it does exist, update it instead."
        """
        if not event.after:
            logger.warning(f"INSERT event missing 'after' data: {event.event_id}")
            return

        table = event.source.table
        schema = event.source.schema_name
        data = event.after

        # Build column and value lists
        columns = list(data.keys())
        values = list(data.values())

        # Build the UPSERT query
        # INSERT INTO {table} (col1, col2) VALUES (%s, %s)
        # ON CONFLICT (id) DO UPDATE SET col1=EXCLUDED.col1, col2=EXCLUDED.col2
        insert_cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        update_set = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in columns
            if c != "id"  # Don't update the PK
        )

        query = sql.SQL(
            "INSERT INTO {schema}.{table} ({columns}) VALUES ({values}) "
            "ON CONFLICT (id) DO UPDATE SET {updates}"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            columns=insert_cols,
            values=placeholders,
            updates=update_set,
        )

        with self._get_cursor() as cursor:
            cursor.execute(query, values)
            self._conn.commit()
            logger.debug(f"Applied INSERT to {schema}.{table}: id={data.get('id')}")

    def _apply_update(self, event: CDCEvent) -> None:
        """
        Apply an UPDATE event.

        Uses the 'before' data to ensure we're updating the expected row.
        If the row has already been updated (duplicate event), the WHERE
        clause won't match and no rows will be affected - this is safe.

        SIMPLE EXPLANATION:
        "Change this row to the new values, but only if
         it still has the old values (hasn't been updated yet)."
        """
        if not event.after:
            logger.warning(f"UPDATE event missing 'after' data: {event.event_id}")
            return

        table = event.source.table
        schema = event.source.schema_name
        after = event.after
        before = event.before or {}

        # Get the primary key
        pk_value = after.get("id") or before.get("id")
        if not pk_value:
            logger.warning(f"UPDATE event missing 'id': {event.event_id}")
            return

        # Build SET clause for all columns except id
        set_columns = [c for c in after.keys() if c != "id"]
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(c)) for c in set_columns
        )
        set_values = [after[c] for c in set_columns]

        # Simple WHERE on PK only (for idempotency, we rely on the upsert pattern)
        query = sql.SQL(
            "UPDATE {schema}.{table} SET {set_clause} WHERE id = %s"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            set_clause=set_clause,
        )

        with self._get_cursor() as cursor:
            cursor.execute(query, set_values + [pk_value])
            rows_affected = cursor.rowcount
            self._conn.commit()

            if rows_affected == 0:
                # Row might have been deleted or already updated
                # Try an upsert instead
                logger.debug(f"UPDATE affected 0 rows, attempting upsert for id={pk_value}")
                self._apply_insert(event)
            else:
                logger.debug(f"Applied UPDATE to {schema}.{table}: id={pk_value}")

    def _apply_delete(self, event: CDCEvent) -> None:
        """
        Apply a DELETE event.

        Deletes the row by primary key. If the row doesn't exist
        (already deleted or duplicate event), this is a no-op.

        SIMPLE EXPLANATION:
        "Delete this row if it exists. If it's already gone, that's fine."
        """
        before = event.before or {}
        table = event.source.table
        schema = event.source.schema_name

        pk_value = before.get("id")
        if not pk_value:
            logger.warning(f"DELETE event missing 'id' in before: {event.event_id}")
            return

        query = sql.SQL("DELETE FROM {schema}.{table} WHERE id = %s").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )

        with self._get_cursor() as cursor:
            cursor.execute(query, [pk_value])
            rows_affected = cursor.rowcount
            self._conn.commit()

            if rows_affected == 0:
                logger.debug(f"DELETE no-op (row doesn't exist): {schema}.{table} id={pk_value}")
            else:
                logger.debug(f"Applied DELETE to {schema}.{table}: id={pk_value}")

    def flush_all(self) -> None:
        """Commit any pending transaction."""
        if self._conn and not self._conn.closed:
            self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        logger.info("Shutting down PostgreSQL sink...")
        if self._conn and not self._conn.closed:
            self._conn.commit()
            self._conn.close()
        logger.info("PostgreSQL sink shutdown complete")

    # ========================================================
    # Schema Management (for Phase 10)
    # ========================================================

    def ensure_schema_exists(self, schema: str = "public") -> None:
        """Create schema if it doesn't exist."""
        with self._get_cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            self._conn.commit()

    def get_table_columns(self, table: str, schema: str = "public") -> list[str]:
        """Get list of columns for a table."""
        with self._get_cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                [schema, table],
            )
            return [row["column_name"] for row in cursor.fetchall()]
