"""
CDC Producer - WAL Reader
==========================
Connects to PostgreSQL via the logical replication protocol and reads
change events from the WAL using the pgoutput output plugin.

How it works:
  1. Opens a REPLICATION connection to PostgreSQL
  2. Issues START_REPLICATION on the cdc_slot replication slot
  3. Receives a stream of pgoutput protocol messages
  4. Decodes them into CDCEvent objects
  5. Yields completed transactions (list of CDCEvent) to the caller
  6. Sends LSN feedback to PostgreSQL to advance the slot position

pgoutput message types we handle:
  B = Begin transaction
  C = Commit transaction
  R = Relation (table schema definition)
  I = Insert
  U = Update
  D = Delete
"""

import logging
import struct
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Generator

import psycopg2
import psycopg2.extras

from src.producer.config import (
    CDC_POLL_INTERVAL_MS,
    KEEPALIVE_INTERVAL_S,
    MAX_BUFFER_SIZE,
    PUBLICATION_NAME,
    REPLICATION_SLOT,
    get_replication_dsn,
)
from src.schemas.cdc_event import CDCEvent, OperationType, SourceInfo

logger = logging.getLogger(__name__)


# ============================================================
# Relation (Table Schema) Cache
# ============================================================

@dataclass
class ColumnInfo:
    """Describes a single column in a relation."""
    name: str
    type_oid: int      # PostgreSQL type OID (23 = int4, 25 = text, etc.)
    is_key: bool       # True if this column is part of the primary key


@dataclass
class RelationInfo:
    """Cached schema for a table, populated from pgoutput R messages."""
    oid: int
    schema: str
    table: str
    columns: list[ColumnInfo] = field(default_factory=list)


# ============================================================
# pgoutput Binary Protocol Decoder
# ============================================================

class PgOutputDecoder:
    """
    Decodes the pgoutput binary protocol messages from PostgreSQL WAL.

    The pgoutput protocol uses a binary format where each message starts
    with a 1-byte type identifier followed by message-specific data.
    We decode each type into Python dictionaries matching our CDCEvent schema.
    """

    def __init__(self, database: str):
        self.database = database
        # Cache of OID → RelationInfo (populated from R messages)
        # We need this because I/U/D messages only include the OID, not column names
        self.relations: dict[int, RelationInfo] = {}

    def decode_message(self, data: bytes) -> dict | None:
        """
        Decode a single pgoutput message.
        Returns a dict with 'type' key, or None if message type is unhandled.
        """
        if not data:
            return None

        msg_type = chr(data[0])
        payload = data[1:]  # Everything after the type byte

        if msg_type == "B":
            return self._decode_begin(payload)
        elif msg_type == "C":
            return self._decode_commit(payload)
        elif msg_type == "R":
            return self._decode_relation(payload)
        elif msg_type == "I":
            return self._decode_insert(payload)
        elif msg_type == "U":
            return self._decode_update(payload)
        elif msg_type == "D":
            return self._decode_delete(payload)
        else:
            # Other message types (e.g., T=truncate, O=origin) — ignore for now
            logger.debug(f"Ignoring message type: {msg_type}")
            return None

    def _decode_begin(self, data: bytes) -> dict:
        """
        B = Begin
        Format: LSN(8) + CommitTime(8) + XID(4)
        """
        lsn_int, commit_time_raw, xid = struct.unpack(">QQI", data[:20])
        lsn = self._int_to_lsn(lsn_int)
        # PostgreSQL epoch starts 2000-01-01 (not Unix 1970-01-01)
        commit_time = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp() + commit_time_raw / 1_000_000
        return {
            "type": "BEGIN",
            "lsn": lsn,
            "xid": xid,
            "commit_time": datetime.fromtimestamp(commit_time, tz=timezone.utc),
        }

    def _decode_commit(self, data: bytes) -> dict:
        """
        C = Commit
        Format: Flags(1) + CommitLSN(8) + EndLSN(8) + CommitTime(8)
        """
        flags = data[0]
        commit_lsn_int, end_lsn_int, commit_time_raw = struct.unpack(">QQQ", data[1:25])
        return {
            "type": "COMMIT",
            "commit_lsn": self._int_to_lsn(commit_lsn_int),
            "end_lsn": self._int_to_lsn(end_lsn_int),
        }

    def _decode_relation(self, data: bytes) -> dict:
        """
        R = Relation (table schema)
        Format: OID(4) + Schema(str) + Table(str) + ReplicaIdentity(1) + NumColumns(2) + [columns...]
        Column: Flags(1) + Name(str) + TypeOID(4) + TypeModifier(4)

        This message is sent whenever a new table appears in the stream.
        We cache it so I/U/D messages can look up column names.
        """
        pos = 0
        oid = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4

        schema, pos = self._read_cstring(data, pos)
        table, pos = self._read_cstring(data, pos)

        replica_identity = chr(data[pos])
        pos += 1

        num_columns = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2

        columns = []
        for _ in range(num_columns):
            flags = data[pos]
            pos += 1
            is_key = bool(flags & 0x01)  # Bit 0 = part of replica identity (primary key)

            col_name, pos = self._read_cstring(data, pos)
            type_oid = struct.unpack(">I", data[pos:pos+4])[0]
            pos += 4
            type_modifier = struct.unpack(">i", data[pos:pos+4])[0]
            pos += 4

            columns.append(ColumnInfo(name=col_name, type_oid=type_oid, is_key=is_key))

        relation = RelationInfo(oid=oid, schema=schema, table=table, columns=columns)
        self.relations[oid] = relation

        logger.debug(f"Cached relation: {schema}.{table} (OID={oid}, columns={[c.name for c in columns]})")

        return {
            "type": "RELATION",
            "oid": oid,
            "schema": schema,
            "table": table,
        }

    def _decode_insert(self, data: bytes) -> dict | None:
        """
        I = Insert
        Format: RelationOID(4) + 'N'(1) + TupleData
        TupleData: NumColumns(2) + [ColumnValue...]
        """
        pos = 0
        relation_oid = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4

        # 'N' = New tuple
        if chr(data[pos]) != "N":
            logger.warning(f"Insert message has unexpected tuple type: {chr(data[pos])}")
            return None
        pos += 1

        relation = self.relations.get(relation_oid)
        if not relation:
            logger.warning(f"No relation info for OID {relation_oid}")
            return None

        new_row, _ = self._decode_tuple(data, pos, relation)

        return {
            "type": "INSERT",
            "schema": relation.schema,
            "table": relation.table,
            "after": new_row,
            "before": None,
        }

    def _decode_update(self, data: bytes) -> dict | None:
        """
        U = Update
        Format: RelationOID(4) + TupleType(1) + TupleData [+ 'N'(1) + NewTupleData]

        TupleType can be:
          'K' = Key tuple (old key values only)
          'O' = Old tuple (full old row, requires REPLICA IDENTITY FULL)
          'N' = New tuple (the updated row — always present)
        """
        pos = 0
        relation_oid = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4

        relation = self.relations.get(relation_oid)
        if not relation:
            logger.warning(f"No relation info for OID {relation_oid}")
            return None

        old_row = None
        tuple_type = chr(data[pos])
        pos += 1

        if tuple_type in ("K", "O"):
            # Old row data present (either key-only or full row with REPLICA IDENTITY FULL)
            old_row, pos = self._decode_tuple(data, pos, relation)
            # Skip the 'N' before new tuple
            pos += 1  # Skip 'N'
        # else: tuple_type == 'N' directly (no old row — only primary key was used)

        new_row, _ = self._decode_tuple(data, pos, relation)

        return {
            "type": "UPDATE",
            "schema": relation.schema,
            "table": relation.table,
            "before": old_row,
            "after": new_row,
        }

    def _decode_delete(self, data: bytes) -> dict | None:
        """
        D = Delete
        Format: RelationOID(4) + TupleType(1) + TupleData
        TupleType: 'K' (key only) or 'O' (full row with REPLICA IDENTITY FULL)
        """
        pos = 0
        relation_oid = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4

        tuple_type = chr(data[pos])
        pos += 1

        relation = self.relations.get(relation_oid)
        if not relation:
            logger.warning(f"No relation info for OID {relation_oid}")
            return None

        old_row, _ = self._decode_tuple(data, pos, relation)

        return {
            "type": "DELETE",
            "schema": relation.schema,
            "table": relation.table,
            "before": old_row,
            "after": None,
        }

    def _decode_tuple(self, data: bytes, pos: int, relation: RelationInfo) -> tuple[dict, int]:
        """
        Decode a TupleData structure into a Python dict.
        Format: NumColumns(2) + [ColumnValue...]
        ColumnValue: Type(1) + [Length(4) + Data(Length) if Type='t']
          Type 'n' = null
          Type 'u' = unchanged (for TOAST values in UPDATE)
          Type 't' = text data
        """
        num_columns = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2

        row = {}
        for i, col in enumerate(relation.columns[:num_columns]):
            col_type = chr(data[pos])
            pos += 1

            if col_type == "n":
                # NULL value
                row[col.name] = None
            elif col_type == "u":
                # Unchanged TOAST value (keep as sentinel — unknown)
                row[col.name] = "__unchanged__"
            elif col_type == "t":
                # Text-format value
                length = struct.unpack(">I", data[pos:pos+4])[0]
                pos += 4
                value_bytes = data[pos:pos+length]
                pos += length
                row[col.name] = self._coerce_value(value_bytes.decode("utf-8"), col.type_oid)
            else:
                logger.warning(f"Unknown column data type: {col_type}")
                row[col.name] = None

        return row, pos

    def _coerce_value(self, text_value: str, type_oid: int) -> object:
        """
        Convert the text representation of a PostgreSQL value to a Python type.
        pgoutput sends all values as text; we convert to appropriate Python types.

        Common PostgreSQL type OIDs:
          16  = bool
          20  = int8 (bigint)
          21  = int2 (smallint)
          23  = int4 (integer)
          700 = float4
          701 = float8
          1700 = numeric/decimal
          Others = keep as string
        """
        if text_value is None:
            return None

        # Integer types
        if type_oid in (20, 21, 23):
            try:
                return int(text_value)
            except ValueError:
                return text_value

        # Float types
        if type_oid in (700, 701):
            try:
                return float(text_value)
            except ValueError:
                return text_value

        # Numeric/Decimal — keep as string to preserve precision
        if type_oid == 1700:
            return text_value

        # Boolean
        if type_oid == 16:
            return text_value.lower() in ("t", "true", "yes", "on", "1")

        # Everything else (text, varchar, timestamp, uuid, etc.) — return as string
        return text_value

    @staticmethod
    def _read_cstring(data: bytes, pos: int) -> tuple[str, int]:
        """Read a null-terminated C string from the byte buffer."""
        end = data.index(b"\x00", pos)
        return data[pos:end].decode("utf-8"), end + 1

    @staticmethod
    def _int_to_lsn(lsn_int: int) -> str:
        """Convert a 64-bit integer to PostgreSQL LSN format (X/YYYYYYYY)."""
        high = lsn_int >> 32
        low = lsn_int & 0xFFFFFFFF
        return f"{high:X}/{low:08X}"


# ============================================================
# WAL Reader — Main Class
# ============================================================

class WALReader:
    """
    Reads change events from PostgreSQL WAL via logical replication.

    Yields completed transactions as lists of CDCEvent objects.
    Handles connection management, keepalives, and LSN feedback.
    """

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.decoder = None
        self.last_lsn = 0  # LSN of last confirmed processed transaction
        self._last_keepalive = time.time()

    def connect(self):
        """Open the replication connection to PostgreSQL."""
        dsn = get_replication_dsn()
        logger.info(f"Connecting to PostgreSQL replication slot '{REPLICATION_SLOT}'...")

        self.conn = psycopg2.connect(dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.cursor = self.conn.cursor()
        self.decoder = PgOutputDecoder(database="source_db")

        logger.info("Replication connection established")

    def start_replication(self):
        """Start streaming WAL changes from the replication slot."""
        # options: publication_names tells pgoutput which publication to use
        # proto_version=1 is the protocol version for pgoutput
        self.cursor.start_replication(
            slot_name=REPLICATION_SLOT,
            decode=False,          # We decode the bytes ourselves
            options={
                "proto_version": "1",
                "publication_names": PUBLICATION_NAME,
            },
        )
        logger.info(f"Started replication from slot '{REPLICATION_SLOT}', publication '{PUBLICATION_NAME}'")

    def read_transactions(self) -> Generator[tuple[list[CDCEvent], int], None, None]:
        """
        Generator that yields (events, lsn) tuples.
        Each yield represents one complete PostgreSQL transaction.
        The caller is responsible for calling confirm_lsn(lsn) after processing.
        """
        # Buffer accumulates events within a transaction (between B and C messages)
        transaction_buffer: list[CDCEvent] = []
        current_xid: int | None = None
        current_lsn: str | None = None

        logger.info("Waiting for WAL changes...")

        while True:
            # Poll for next message (non-blocking with timeout)
            message = self.cursor.read_message()

            if message is None:
                # No new messages — send keepalive if needed
                self._maybe_send_keepalive()
                time.sleep(CDC_POLL_INTERVAL_MS / 1000)
                continue

            # message.data_start is the LSN of this message
            msg_lsn_int = message.data_start
            msg_lsn = PgOutputDecoder._int_to_lsn(msg_lsn_int)

            # Decode the pgoutput message
            decoded = self.decoder.decode_message(message.payload)
            if decoded is None:
                # Send feedback to keep the connection alive
                message.cursor.send_feedback(flush_lsn=msg_lsn_int)
                continue

            msg_type = decoded["type"]

            if msg_type == "BEGIN":
                # Start of a new transaction — reset buffer
                transaction_buffer = []
                current_xid = decoded["xid"]
                current_lsn = decoded["lsn"]
                logger.debug(f"BEGIN xid={current_xid} lsn={current_lsn}")

            elif msg_type in ("INSERT", "UPDATE", "DELETE"):
                # Row-level change — build a CDCEvent and buffer it
                op_map = {
                    "INSERT": OperationType.INSERT,
                    "UPDATE": OperationType.UPDATE,
                    "DELETE": OperationType.DELETE,
                }
                event = CDCEvent(
                    source=SourceInfo(
                        database="source_db",
                        schema=decoded["schema"],
                        table=decoded["table"],
                        transaction_id=current_xid,
                        lsn=msg_lsn,
                    ),
                    operation=op_map[msg_type],
                    before=decoded.get("before"),
                    after=decoded.get("after"),
                )
                transaction_buffer.append(event)

                # Safety valve: flush early if buffer is too large
                if len(transaction_buffer) >= MAX_BUFFER_SIZE:
                    logger.warning(
                        f"Transaction buffer hit MAX_BUFFER_SIZE ({MAX_BUFFER_SIZE}). "
                        "Flushing early — this transaction may produce partial Kafka batches."
                    )
                    yield transaction_buffer[:], msg_lsn_int
                    transaction_buffer.clear()

            elif msg_type == "COMMIT":
                # End of transaction — yield the buffered events
                if transaction_buffer:
                    commit_lsn = decoded["commit_lsn"]
                    # Convert LSN string back to int for feedback
                    commit_lsn_parts = commit_lsn.split("/")
                    commit_lsn_int = (int(commit_lsn_parts[0], 16) << 32) | int(commit_lsn_parts[1], 16)

                    logger.debug(
                        f"COMMIT xid={current_xid} lsn={commit_lsn} "
                        f"events={len(transaction_buffer)}"
                    )

                    yield transaction_buffer[:], commit_lsn_int
                    transaction_buffer.clear()
                else:
                    # Empty transaction (e.g., BEGIN/COMMIT with no DML) — just advance LSN
                    commit_lsn = decoded["commit_lsn"]
                    commit_lsn_parts = commit_lsn.split("/")
                    commit_lsn_int = (int(commit_lsn_parts[0], 16) << 32) | int(commit_lsn_parts[1], 16)
                    self.cursor.send_feedback(flush_lsn=commit_lsn_int)

            elif msg_type == "RELATION":
                # Schema update — already handled by decoder, no action needed here
                logger.debug(f"Schema cached: {decoded['schema']}.{decoded['table']}")

    def confirm_lsn(self, lsn_int: int):
        """
        Tell PostgreSQL that we've successfully processed up to this LSN.
        This allows the replication slot to advance and WAL to be cleaned up.
        ONLY call this after Kafka has confirmed the events were produced.
        """
        self.cursor.send_feedback(flush_lsn=lsn_int)
        self.last_lsn = lsn_int
        logger.debug(f"Confirmed LSN: {PgOutputDecoder._int_to_lsn(lsn_int)}")

    def _maybe_send_keepalive(self):
        """Send a keepalive message if the connection has been idle too long."""
        now = time.time()
        if now - self._last_keepalive >= KEEPALIVE_INTERVAL_S:
            if self.cursor and self.last_lsn > 0:
                self.cursor.send_feedback(flush_lsn=self.last_lsn, reply=True)
            self._last_keepalive = now

    def close(self):
        """Close the replication connection."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Replication connection closed")
        except Exception as e:
            logger.warning(f"Error closing replication connection: {e}")
