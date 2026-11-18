"""Correctable CDC window reports built on top of native row events.

This module adds an additive reporting layer.  It consumes *whole transactions*
of native row events (the objects produced by
:class:`~pymysqlreplication.binlogstream.BinLogStreamReader`) together with the
trailing ``file``/``pos`` coordinate of each transaction, and maintains windowed
aggregates that stay correct when late, re-classifying or replaying changes
arrive.  The reader, the event classes and the existing public API are left
untouched.

Semantics
=========

Business time
    Every row carries a business timestamp taken from ``time_column``.  The
    value is interpreted as UTC: naive :class:`datetime.datetime` objects are
    assumed to be UTC and aware ones are converted to UTC.  The transaction or
    commit time (``event.timestamp``) is never used as a substitute.

Windows
    Windows have a fixed width, are aligned on ``window_origin`` and are half
    open (``[start, end)``).  ``window_id`` is the ISO-8601 UTC representation
    of the window start, so it is stable across process restarts.  Open windows
    are never published.

Aggregates
    For every ``(window_id, group_key)`` pair the report maintains
    ``count_all`` (number of live rows, including rows whose amount is
    ``NULL``) and ``sum_amount`` (the exact sum of the non-``NULL`` integer
    amounts, ``NULL`` when the group has no non-``NULL`` amount).  Floats and
    decimals are rejected so that the sum is always exact.

Corrections
    Every tracked row keeps its last known state, keyed by the primary key
    declared by the table map.  ``binlog_row_image=MINIMAL`` images are
    reconstructed from that state: an update only carries the primary key in
    its before image and the changed columns in its after image, a delete only
    carries the primary key.  Moving a row to another group, moving it to
    another business time and deleting it retract the contribution from the
    *original* window/group before the new contribution is added, so previously
    published windows are corrected instead of silently drifting.

Watermark
    Windows are sealed by an explicitly supplied, monotonically increasing
    watermark.  A window ``[start, end)`` is sealed once ``end <= watermark``.
    Commit timestamps are never used as a watermark.

Revisions
    Closing a window publishes its complete statistics with revision ``1``.
    A late change that alters an already sealed window publishes one record per
    changed group with the *same* ``window_id`` and an incremented revision.
    When the recomputed result is identical to the last published result
    nothing is republished.  A group that becomes empty is published as
    ``count_all=0`` and ``sum_amount=None``.

Transactions and durability
    A source transaction is processed on staged copies and only its final state
    is published.  Facts, aggregates, window state, the cursor-readable report
    events and the ``(file, pos)`` checkpoint are committed together; on any
    failure the previous state is kept.  Replayed transactions (same trailing
    coordinate or transaction id) and reopened stores never apply a correction
    twice.
"""

import base64
import collections
import copy
import datetime
import decimal
import json
import os
import tempfile
import threading

from .constants import NONE_SOURCE
from .constants import BINLOG

__all__ = [
    "Checkpoint",
    "Transaction",
    "WindowRecord",
    "WindowReport",
    "CorrectableWindowReport",
    "CDCWindowReport",
    "WindowReportError",
    "WatermarkRegressionError",
    "MissingPrimaryKeyError",
    "UnresolvedRowStateError",
    "InvalidAmountError",
    "InvalidBusinessTimeError",
]

_UTC = datetime.timezone.utc

# Binlog row event types understood by the report (V1 and V2 variants).
_WRITE_EVENT_TYPES = frozenset(
    (
        BINLOG.WRITE_ROWS_EVENT_V1,
        BINLOG.WRITE_ROWS_EVENT_V2,
        getattr(BINLOG, "PRE_GA_WRITE_ROWS_EVENT", object()),
    )
)
_UPDATE_EVENT_TYPES = frozenset(
    (
        BINLOG.UPDATE_ROWS_EVENT_V1,
        BINLOG.UPDATE_ROWS_EVENT_V2,
        getattr(BINLOG, "PRE_GA_UPDATE_ROWS_EVENT", object()),
        getattr(BINLOG, "PARTIAL_UPDATE_ROWS_EVENT", object()),
    )
)
_DELETE_EVENT_TYPES = frozenset(
    (
        BINLOG.DELETE_ROWS_EVENT_V1,
        BINLOG.DELETE_ROWS_EVENT_V2,
        getattr(BINLOG, "PRE_GA_DELETE_ROWS_EVENT", object()),
    )
)

_ROW_KIND_WRITE = "write"
_ROW_KIND_UPDATE = "update"
_ROW_KIND_DELETE = "delete"


class WindowReportError(Exception):
    """Base class for errors raised by :class:`WindowReport`."""


class WatermarkRegressionError(WindowReportError):
    """Raised when a watermark older than the current watermark is supplied."""


class MissingPrimaryKeyError(WindowReportError):
    """Raised when a row cannot be identified because no primary key is known."""


class UnresolvedRowStateError(WindowReportError):
    """Raised when a MINIMAL row image cannot be completed from tracked state."""


class InvalidAmountError(WindowReportError):
    """Raised when an amount is not an integer (or ``None``)."""


class InvalidBusinessTimeError(WindowReportError):
    """Raised when the business timestamp column is missing or not a datetime."""


def _as_utc(value, *, what="datetime"):
    """Normalize ``value`` to an aware UTC :class:`datetime.datetime`."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=_UTC)
        return value.astimezone(_UTC)
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day, tzinfo=_UTC)
    if isinstance(value, bool):
        raise TypeError("%s must be a datetime, got bool" % what)
    if isinstance(value, (int, float)):
        return datetime.datetime.fromtimestamp(value, tz=_UTC)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return _as_utc(datetime.datetime.fromisoformat(text), what=what)
        except ValueError:
            raise TypeError("%s is not an ISO-8601 datetime: %r" % (what, value))
    raise TypeError(
        "%s must be a datetime, date, epoch or ISO-8601 string, got %s"
        % (what, type(value).__name__)
    )


def _as_width(value):
    """Normalize a window width to a positive :class:`datetime.timedelta`."""
    if isinstance(value, datetime.timedelta):
        width = value
    elif isinstance(value, bool):
        raise TypeError("window_width must be a timedelta or seconds, got bool")
    elif isinstance(value, (int, float)):
        width = datetime.timedelta(seconds=value)
    else:
        raise TypeError(
            "window_width must be a timedelta or a number of seconds, got %s"
            % type(value).__name__
        )
    if width <= datetime.timedelta(0):
        raise ValueError("window_width must be positive")
    return width


def _hashable(value):
    """Return a hashable, deterministic representation of a row value."""
    if isinstance(value, list):
        return tuple(_hashable(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((_hashable(k), _hashable(v)) for k, v in value.items()))
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable(item) for item in value)
    if isinstance(value, bytearray):
        return bytes(value)
    try:
        hash(value)
    except TypeError:
        raise TypeError("row value is not hashable: %r" % (value,))
    return value


def _validate_amount(value):
    """Validate an amount, returning an exact integer or ``None``."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise InvalidAmountError(
            "amounts must be integers or None for an exact sum, got bool"
        )
    if isinstance(value, int):
        return value
    if isinstance(value, decimal.Decimal):
        # A DECIMAL that is not integral would make the sum inexact.
        if value != value.to_integral_value():
            raise InvalidAmountError(
                "amounts must be integers or None for an exact sum, got %r" % (value,)
            )
        return int(value)
    raise InvalidAmountError(
        "amounts must be integers or None for an exact sum, got %s"
        % type(value).__name__
    )


def _group_of(values, columns):
    key = []
    for column in columns:
        if column not in values:
            raise WindowReportError(
                "group column %r is missing from the row" % (column,)
            )
        key.append(_hashable(values[column]))
    return tuple(key)


def _encode(value):
    """Encode an arbitrary row value for JSON persistence."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return {"$f": repr(value)}
    if isinstance(value, datetime.datetime):
        return {"$dt": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$d": value.isoformat()}
    if isinstance(value, datetime.time):
        return {"$t": value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {"$td": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, decimal.Decimal):
        return {"$dec": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"$b": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(item) for item in value]}
    if isinstance(value, list):
        return {"$list": [_encode(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        items = [_encode(item) for item in value]
        items.sort(key=repr)
        return {"$set": items}
    if isinstance(value, dict):
        return {
            "$dict": [
                [_encode(k), _encode(v)] for k, v in value.items()
            ]
        }
    return {"$repr": repr(value)}


def _decode(value):
    """Inverse of :func:`_encode`."""
    if not isinstance(value, dict):
        return value
    if "$f" in value:
        return float(value["$f"])
    if "$dt" in value:
        return datetime.datetime.fromisoformat(value["$dt"])
    if "$d" in value:
        return datetime.date.fromisoformat(value["$d"])
    if "$t" in value:
        return datetime.time.fromisoformat(value["$t"])
    if "$td" in value:
        return datetime.timedelta(*value["$td"])
    if "$dec" in value:
        return decimal.Decimal(value["$dec"])
    if "$b" in value:
        return base64.b64decode(value["$b"].encode("ascii"))
    if "$tuple" in value:
        return tuple(_decode(item) for item in value["$tuple"])
    if "$list" in value:
        return [_decode(item) for item in value["$list"]]
    if "$set" in value:
        return set(_decode(item) for item in value["$set"])
    if "$dict" in value:
        return {_decode(k): _decode(v) for k, v in value["$dict"]}
    if "$repr" in value:
        return value["$repr"]
    return value


def _group_to_json(group):
    return [_encode(item) for item in group]


def _group_from_json(items):
    return tuple(_decode(item) for item in items)


class Checkpoint:
    """Trailing ``(file, pos)`` coordinate of a committed source transaction."""

    __slots__ = ("log_file", "log_pos")

    def __init__(self, log_file, log_pos):
        if not isinstance(log_file, str) or not log_file:
            raise ValueError("log_file must be a non-empty string")
        if isinstance(log_pos, bool) or not isinstance(log_pos, int):
            raise TypeError("log_pos must be an integer")
        self.log_file = log_file
        self.log_pos = log_pos

    @property
    def key(self):
        return (self.log_file, self.log_pos)

    def __eq__(self, other):
        if isinstance(other, Checkpoint):
            return self.key == other.key
        return NotImplemented

    def __hash__(self):
        return hash(self.key)

    def __repr__(self):
        return "Checkpoint(%r, %r)" % (self.log_file, self.log_pos)

    def __iter__(self):
        return iter(self.key)


class Transaction:
    """A complete source transaction and its trailing file/pos coordinate.

    ``events`` must already be split per transaction by the caller.  The report
    consumes the row events in order but only publishes the final state of the
    transaction.
    """

    __slots__ = ("events", "log_file", "log_pos", "transaction_id")

    def __init__(self, events, log_file=None, log_pos=None, transaction_id=None):
        self.events = tuple(events)
        self.log_file = log_file
        self.log_pos = log_pos
        self.transaction_id = transaction_id

    @classmethod
    def coerce(cls, value):
        """Build a transaction from a sequence or a duck-typed transaction."""
        if isinstance(value, cls):
            return value
        if hasattr(value, "events"):
            return cls(
                value.events,
                log_file=getattr(value, "log_file", None),
                log_pos=getattr(value, "log_pos", None),
                transaction_id=getattr(value, "transaction_id", None),
            )
        return cls(value)

    @property
    def checkpoint(self):
        if self.log_file is None and self.log_pos is None:
            return None
        if self.log_file is None or self.log_pos is None:
            raise ValueError("log_file and log_pos must be provided together")
        return Checkpoint(self.log_file, self.log_pos)


class WindowRecord:
    """One published window statistic (a single group revision)."""

    __slots__ = (
        "sequence",
        "window_id",
        "window_start",
        "window_end",
        "group_key",
        "count_all",
        "sum_amount",
        "revision",
    )

    def __init__(
        self,
        sequence,
        window_id,
        window_start,
        window_end,
        group_key,
        count_all,
        sum_amount,
        revision,
    ):
        self.sequence = sequence
        self.window_id = window_id
        self.window_start = window_start
        self.window_end = window_end
        self.group_key = group_key
        self.count_all = count_all
        self.sum_amount = sum_amount
        self.revision = revision

    # ``windowID``/``groupKey``/``sum`` are the spellings used by the report
    # contract; the snake_case attributes remain the canonical Python names.
    @property
    def windowID(self):
        return self.window_id

    @property
    def groupKey(self):
        return self.group_key

    @property
    def sum(self):
        return self.sum_amount

    @property
    def countAll(self):
        return self.count_all

    def as_dict(self):
        return {
            "sequence": self.sequence,
            "window_id": self.window_id,
            "windowID": self.window_id,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "group_key": None
            if self.group_key is None
            else _group_to_json(self.group_key),
            "groupKey": None
            if self.group_key is None
            else _group_to_json(self.group_key),
            "count_all": self.count_all,
            "sum_amount": self.sum_amount,
            "sum": self.sum_amount,
            "revision": self.revision,
        }

    @classmethod
    def from_dict(cls, data):
        window_id = data.get("window_id", data.get("windowID"))
        group_key = data.get("group_key", data.get("groupKey"))
        sum_amount = data.get("sum_amount", data.get("sum"))
        return cls(
            sequence=data["sequence"],
            window_id=window_id,
            window_start=datetime.datetime.fromisoformat(data["window_start"]),
            window_end=datetime.datetime.fromisoformat(data["window_end"]),
            group_key=None if group_key is None else _group_from_json(group_key),
            count_all=data["count_all"],
            sum_amount=sum_amount,
            revision=data["revision"],
        )

    @property
    def window_start_utc(self):
        return self.window_start

    @property
    def window_end_utc(self):
        return self.window_end

    def __eq__(self, other):
        if not isinstance(other, WindowRecord):
            return NotImplemented
        return self.as_dict() == other.as_dict()

    def __repr__(self):
        return (
            "WindowRecord(sequence=%r, window_id=%r, group_key=%r, "
            "count_all=%r, sum_amount=%r, revision=%r)"
            % (
                self.sequence,
                self.window_id,
                self.group_key,
                self.count_all,
                self.sum_amount,
                self.revision,
            )
        )


class _RowState:
    """Last known state of one live source row."""

    __slots__ = (
        "schema",
        "table",
        "pk",
        "values",
        "business_time",
        "window_id",
        "group_key",
        "amount",
    )

    def __init__(
        self,
        schema,
        table,
        pk,
        values,
        business_time,
        window_id,
        group_key,
        amount,
    ):
        self.schema = schema
        self.table = table
        self.pk = pk
        self.values = values
        self.business_time = business_time
        self.window_id = window_id
        self.group_key = group_key
        self.amount = amount

    def clone(self):
        return _RowState(
            self.schema,
            self.table,
            self.pk,
            copy.deepcopy(self.values),
            self.business_time,
            self.window_id,
            self.group_key,
            self.amount,
        )

    @classmethod
    def from_dict(cls, data):
        return cls(
            schema=data["schema"],
            table=data["table"],
            pk=_group_from_json(data["pk"]),
            values={key: _decode(value) for key, value in data["values"].items()},
            business_time=datetime.datetime.fromisoformat(data["business_time"]),
            window_id=datetime.datetime.fromisoformat(data["window_id"]),
            group_key=_group_from_json(data["group_key"]),
            amount=data["amount"],
        )

    def as_dict(self):
        return {
            "schema": self.schema,
            "table": self.table,
            "pk": _group_to_json(self.pk),
            "values": {key: _encode(value) for key, value in self.values.items()},
            "business_time": self.business_time.isoformat(),
            "window_id": self.window_id.isoformat(),
            "group_key": _group_to_json(self.group_key),
            "amount": self.amount,
        }


class _WindowState:
    """Per-window aggregates, sealing status and publication history."""

    __slots__ = (
        "start",
        "end",
        "closed",
        "revision",
        "aggregates",
        "known_groups",
        "last_emitted",
    )

    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.closed = False
        self.revision = 0
        # group_key -> [count_all, non_null_count, exact_sum]
        self.aggregates = {}
        self.known_groups = set()
        # group_key -> (count_all, sum_amount)
        self.last_emitted = {}

    # -- aggregate maintenance ------------------------------------------------
    def apply(self, group_key, count_delta, non_null_delta, sum_delta):
        """Apply an aggregate delta and register the group as known."""
        stats = self.aggregates.get(group_key)
        if stats is None:
            stats = [0, 0, 0]
            self.aggregates[group_key] = stats
        stats[0] += count_delta
        stats[1] += non_null_delta
        stats[2] += sum_delta
        self.known_groups.add(group_key)
        if stats[0] < 0 or stats[1] < 0 or stats[1] > stats[0]:
            raise WindowReportError(
                "window aggregate became inconsistent for group %r" % (group_key,)
            )

    def result(self, group_key):
        stats = self.aggregates.get(group_key)
        if stats is None or stats[0] <= 0:
            return (0, None)
        return (stats[0], stats[2] if stats[1] > 0 else None)

    def groups(self):
        return set(self.aggregates) | set(self.last_emitted) | set(self.known_groups)

    def clone(self):
        other = _WindowState(self.start, self.end)
        other.closed = self.closed
        other.revision = self.revision
        other.aggregates = {
            key: list(value) for key, value in self.aggregates.items()
        }
        other.known_groups = set(self.known_groups)
        other.last_emitted = dict(self.last_emitted)
        return other

    @classmethod
    def from_dict(cls, data):
        other = cls(
            datetime.datetime.fromisoformat(data["start"]),
            datetime.datetime.fromisoformat(data["end"]),
        )
        other.closed = data["closed"]
        other.revision = data["revision"]
        for entry in data["aggregates"]:
            other.aggregates[_group_from_json(entry["group_key"])] = list(
                entry["stats"]
            )
        other.known_groups = {
            _group_from_json(items) for items in data["known_groups"]
        }
        for entry in data["last_emitted"]:
            other.last_emitted[_group_from_json(entry["group_key"])] = (
                entry["count_all"],
                entry["sum_amount"],
            )
        return other

    def as_dict(self):
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "closed": self.closed,
            "revision": self.revision,
            "aggregates": [
                {"group_key": _group_to_json(key), "stats": list(stats)}
                for key, stats in self.aggregates.items()
            ],
            "known_groups": [_group_to_json(key) for key in self.known_groups],
            "last_emitted": [
                {
                    "group_key": _group_to_json(key),
                    "count_all": value[0],
                    "sum_amount": value[1],
                }
                for key, value in self.last_emitted.items()
            ],
        }

_STORE_VERSION = 1


class CorrectableWindowReport:
    """Windowed, correctable aggregates over native binlog row events.

    Parameters
    ----------
    window_width:
        Fixed window width, either a :class:`datetime.timedelta` or a number of
        seconds.  Must be positive.
    window_origin:
        Origin used to align windows.  Defaults to ``1970-01-01T00:00:00Z``.
    time_column:
        Name of the column holding the business timestamp.  The value is
        interpreted as UTC (see the module docstring).
    amount_column:
        Name of the nullable integer amount column.
    group_columns:
        Column name or sequence of column names forming ``group_key``.  An
        empty key is used when omitted.
    """

    def __init__(
        self,
        window_width,
        window_origin=datetime.datetime(1970, 1, 1, tzinfo=_UTC),
        time_column="ts",
        amount_column="amount",
        group_columns=(),
        group_column=None,
        store=None,
    ):
        self._width = _as_width(window_width)
        self._origin = _as_utc(window_origin, what="window_origin")
        self._time_column = time_column
        self._amount_column = amount_column
        if group_column is not None:
            if group_columns:
                raise TypeError("use either group_columns or group_column, not both")
            group_columns = (group_column,)
        if isinstance(group_columns, str):
            group_columns = (group_columns,)
        self._group_columns = tuple(group_columns)

        self._lock = threading.RLock()
        self._rows = {}
        self._windows = {}
        self._records = []
        self._sequence = 0
        self._watermark = None
        self._last_checkpoint = None
        self._applied_ids = collections.OrderedDict()
        self._applied_checkpoints = collections.OrderedDict()
        if store is not None:
            self.load(store)

    # -- configuration --------------------------------------------------------
    @property
    def window_width(self):
        return self._width

    @property
    def window_origin(self):
        return self._origin

    @property
    def time_column(self):
        return self._time_column

    @property
    def amount_column(self):
        return self._amount_column

    @property
    def group_columns(self):
        return self._group_columns

    # -- watermark ------------------------------------------------------------
    @property
    def watermark(self):
        return self._watermark

    def advance_watermark(self, watermark):
        """Seal every window ending at or before ``watermark``.

        The watermark is explicit data supplied by the caller and is never
        derived from the commit timestamps of the transactions.  It is
        monotonic: a smaller watermark raises
        :class:`WatermarkRegressionError` and leaves the report unchanged.

        Sealing publishes the complete statistics of every newly sealed window
        with revision ``1``.  Open windows are not published early.  Returns
        the published :class:`WindowRecord` objects (possibly empty).
        """
        watermark = _as_utc(watermark, what="watermark")
        with self._lock:
            if self._watermark is not None and watermark < self._watermark:
                raise WatermarkRegressionError(
                    "watermark %s is older than the current watermark %s"
                    % (watermark.isoformat(), self._watermark.isoformat())
                )
            publishable = []
            if self._watermark is None or watermark > self._watermark:
                for window_id in sorted(self._windows):
                    committed = self._windows[window_id]
                    if committed.closed or committed.end > watermark:
                        continue
                    candidate = committed.clone()
                    candidate.closed = True
                    emitted = self._publish_window(candidate, candidate.groups())
                    publishable.append((window_id, candidate, emitted))
            return self._commit_publishable(
                publishable, watermark=watermark, checkpoint=None
            )

    def set_watermark(self, watermark):
        """Alias of :meth:`advance_watermark`."""
        return self.advance_watermark(watermark)

    seal = advance_watermark
    close_windows = advance_watermark
    apply_watermark = advance_watermark
    watermark_advance = advance_watermark

    # -- transaction intake ---------------------------------------------------
    def accept(self, transaction, log_file=None, log_pos=None, transaction_id=None):
        """Consume one complete source transaction.

        ``transaction`` may be a :class:`Transaction`, or a sequence of events
        in which case ``log_file``/``log_pos``/``transaction_id`` describe it.
        Events must already be split per transaction; the report only publishes
        the final state of the transaction, never intermediate states.

        Returns the :class:`WindowRecord` objects published by the transaction
        (possibly empty).  On failure the previous state is kept intact.
        """
        if not isinstance(transaction, Transaction):
            if hasattr(transaction, "events"):
                if log_file is not None or log_pos is not None or transaction_id is not None:
                    raise TypeError(
                        "file/pos and transaction id must live on the transaction"
                    )
                transaction = Transaction.coerce(transaction)
            else:
                transaction = Transaction(
                    transaction,
                    log_file=log_file,
                    log_pos=log_pos,
                    transaction_id=transaction_id,
                )
        elif log_file is not None or log_pos is not None or transaction_id is not None:
            raise TypeError(
                "file/pos and transaction id must live on the Transaction object"
            )
        checkpoint = transaction.checkpoint
        with self._lock:
            if self._is_replay(transaction, checkpoint):
                return []
            staged_rows = {}
            deltas = {}
            for event in transaction.events:
                self._consume_event(event, staged_rows, deltas)
            publishable = self._stage_publication(deltas)
            return self._commit_publishable(
                publishable,
                transaction=transaction,
                checkpoint=checkpoint,
                staged_rows=staged_rows,
            )

    # Convenience aliases -----------------------------------------------------
    apply = accept
    ingest = accept
    consume = accept
    add_transaction = accept
    accept_transaction = accept

    # -- replay protection ----------------------------------------------------
    def _is_replay(self, transaction, checkpoint):
        if (
            transaction.transaction_id is not None
            and transaction.transaction_id in self._applied_ids
        ):
            return True
        if checkpoint is not None:
            if checkpoint.key in self._applied_checkpoints:
                return True
            # A resume from an older position replays everything up to the last
            # committed coordinate; those transactions must not be applied again.
            if (
                self._last_checkpoint is not None
                and checkpoint.key <= self._last_checkpoint
            ):
                return True
        return False

    def _remember_transaction(self, transaction, checkpoint):
        if transaction.transaction_id is not None:
            self._applied_ids[transaction.transaction_id] = checkpoint
            self._applied_ids.move_to_end(transaction.transaction_id)
            while len(self._applied_ids) > 4096:
                self._applied_ids.popitem(last=False)
        if checkpoint is not None:
            self._applied_checkpoints[checkpoint.key] = True
            self._applied_checkpoints.move_to_end(checkpoint.key)
            while len(self._applied_checkpoints) > 4096:
                self._applied_checkpoints.popitem(last=False)
            if (
                self._last_checkpoint is None
                or checkpoint.key > self._last_checkpoint
            ):
                self._last_checkpoint = checkpoint.key

    @property
    def checkpoint(self):
        """Last committed ``(file, pos)`` coordinate, or ``None``."""
        if self._last_checkpoint is None:
            return None
        return Checkpoint(*self._last_checkpoint)

    # -- window helpers -------------------------------------------------------
    def _width_us(self):
        return (
            self._width.days * 86400 * 1000000
            + self._width.seconds * 1000000
            + self._width.microseconds
        )

    def _window_bounds(self, business_time):
        business_time = _as_utc(business_time, what="business time")
        delta = business_time - self._origin
        offset_us = (
            delta.days * 86400 * 1000000
            + delta.seconds * 1000000
            + delta.microseconds
        )
        width_us = self._width_us()
        index = offset_us // width_us
        start = self._origin + datetime.timedelta(microseconds=index * width_us)
        return start, start + self._width

    def _window_for_id(self, window_id):
        window = self._windows.get(window_id)
        if window is None:
            start = datetime.datetime.fromisoformat(window_id)
            window = _WindowState(start, start + self._width)
        return window

    # -- row extraction -------------------------------------------------------
    @staticmethod
    def _event_kind(event):
        """Classify a row event as write/update/delete, or ``None``."""
        event_type = getattr(event, "event_type", None)
        if event_type in _UPDATE_EVENT_TYPES:
            return _ROW_KIND_UPDATE
        if event_type in _DELETE_EVENT_TYPES:
            return _ROW_KIND_DELETE
        if event_type in _WRITE_EVENT_TYPES:
            return _ROW_KIND_WRITE
        # Duck-typed events without an event type: fall back to the class name.
        name = type(event).__name__
        if "Update" in name:
            return _ROW_KIND_UPDATE
        if "Delete" in name:
            return _ROW_KIND_DELETE
        if "Write" in name or "Insert" in name:
            return _ROW_KIND_WRITE
        return None

    @classmethod
    def _row_images(cls, event):
        """Yield ``(kind, before, after)`` for every row of a row event."""
        try:
            rows = event.rows
        except AttributeError:
            return
        if not rows:
            return
        kind = cls._event_kind(event)
        for row in rows:
            row_kind = kind
            if row_kind is None:
                if "before_values" in row and "after_values" in row:
                    row_kind = _ROW_KIND_UPDATE
                elif "values" in row:
                    row_kind = _ROW_KIND_WRITE
                else:
                    continue
            if row_kind == _ROW_KIND_UPDATE:
                yield _ROW_KIND_UPDATE, row, row
            elif row_kind == _ROW_KIND_DELETE:
                yield _ROW_KIND_DELETE, row, None
            else:
                yield _ROW_KIND_WRITE, None, row

    @staticmethod
    def _present_values(values, none_sources):
        """Columns carried by a row image (``COLS_BITMAP`` columns are absent)."""
        present = {}
        for column, value in values.items():
            source = none_sources.get(column)
            if source in (NONE_SOURCE.COLS_BITMAP, NONE_SOURCE.JSON_PARTIAL_UPDATE):
                continue
            present[column] = value
        return present

    @staticmethod
    def _pk_columns(event):
        pk = getattr(event, "primary_key", None)
        if not pk:
            raise MissingPrimaryKeyError(
                "%s.%s has no declared primary key; a stable row identity is "
                "required to correct MINIMAL row images"
                % (getattr(event, "schema", "?"), getattr(event, "table", "?"))
            )
        if isinstance(pk, str):
            return [pk]
        columns = list(pk)
        if not columns:
            raise MissingPrimaryKeyError("empty primary key")
        return columns

    @staticmethod
    def _is_complete(values, none_sources):
        """True when the row image carries every column of the row.

        With ``binlog_row_image=MINIMAL`` the columns missing from the image
        are flagged with :data:`NONE_SOURCE.COLS_BITMAP`; any such flag means
        the image alone cannot reconstruct the previous state.
        """
        if not values:
            return False
        return not any(
            source == NONE_SOURCE.COLS_BITMAP for source in none_sources.values()
        )

    def _pk_from(self, values, columns):
        pk = []
        for column in columns:
            if column not in values:
                raise UnresolvedRowStateError(
                    "primary key column %r cannot be resolved from the row "
                    "image or the tracked state" % (column,)
                )
            pk.append(_hashable(values[column]))
        return tuple(pk)

    def _pk_of(self, values, none_sources, columns):
        pk = []
        for column in columns:
            if column not in values or none_sources.get(column) == NONE_SOURCE.COLS_BITMAP:
                raise UnresolvedRowStateError(
                    "primary key column %r is absent from the row image" % (column,)
                )
            pk.append(_hashable(values[column]))
        return tuple(pk)

    # -- event consumption (staged) -------------------------------------------
    def _consume_event(self, event, staged_rows, deltas):
        schema = getattr(event, "schema", None)
        table = getattr(event, "table", None)
        pk_columns = None
        for kind, before_row, after_row in self._row_images(event):
            if pk_columns is None:
                pk_columns = self._pk_columns(event)
            if kind == _ROW_KIND_DELETE:
                values = before_row["values"]
                sources = before_row.get("none_sources", {})
                pk = self._pk_of(values, sources, pk_columns)
                self._delete_row(schema, table, pk, values, sources, staged_rows, deltas)
                continue

            if kind == _ROW_KIND_WRITE:
                values = after_row["values"]
                sources = after_row.get("none_sources", {})
                pk = self._pk_of(values, sources, pk_columns)
                previous = self._staged_get(staged_rows, schema, table, pk)
                merged = dict(previous.values) if previous is not None else {}
                merged.update(self._present_values(values, sources))
                self._replace_row(schema, table, pk, merged, staged_rows, deltas)
                continue

            before_values = before_row["before_values"]
            before_sources = before_row.get("before_none_sources", {})
            after_values = after_row["after_values"]
            after_sources = after_row.get("after_none_sources", {})
            old_pk = self._pk_of(before_values, before_sources, pk_columns)
            before_present = self._present_values(before_values, before_sources)
            after_present = self._present_values(after_values, after_sources)
            previous = self._staged_get(staged_rows, schema, table, old_pk)
            if previous is not None:
                old_state = previous
                merged = dict(previous.values)
            elif self._is_complete(before_values, before_sources):
                # The report started mid-stream but the before image is full:
                # it still describes the contribution that must be retracted.
                old_state = self._build_state(schema, table, old_pk, before_present)
                merged = dict(old_state.values)
            else:
                raise UnresolvedRowStateError(
                    "cannot reconstruct the previous state of %s.%s row %r; the "
                    "row is not tracked and the before image is MINIMAL"
                    % (schema, table, old_pk)
                )
            merged.update(after_present)
            try:
                new_pk = self._pk_from(merged, pk_columns)
            except UnresolvedRowStateError:
                # A MINIMAL after image that does not carry the key leaves the
                # key unchanged.
                new_pk = old_pk
            new_state = self._build_state(schema, table, new_pk, merged)
            self._apply_replacement(staged_rows, deltas, old_state, new_state)

    def _replace_row(
        self, schema, table, pk, values, staged_rows, deltas
    ):
        """Insert or overwrite a row (writes always carry the full new image)."""
        previous = self._staged_get(staged_rows, schema, table, pk)
        new_state = self._build_state(schema, table, pk, values)
        self._apply_replacement(staged_rows, deltas, previous, new_state)

    def _apply_replacement(self, staged_rows, deltas, old_state, new_state):
        """Atomically stage ``old_state`` -> ``new_state`` and its deltas."""
        if old_state is not None:
            self._retract(deltas, old_state)
            key = self._row_key(
                old_state.schema, old_state.table, old_state.pk
            )
            if key != self._row_key(
                new_state.schema, new_state.table, new_state.pk
            ):
                self._staged_delete(
                    staged_rows,
                    old_state.schema,
                    old_state.table,
                    old_state.pk,
                )
        if new_state.pk != getattr(old_state, "pk", None) or old_state is None:
            existing = self._staged_get(
                staged_rows, new_state.schema, new_state.table, new_state.pk
            )
            if existing is not None and existing is not old_state:
                self._retract(deltas, existing)
        self._staged_set(
            staged_rows, new_state.schema, new_state.table, new_state.pk, new_state
        )
        self._contribute(deltas, new_state)

    def _delete_row(self, schema, table, pk, values, sources, staged_rows, deltas):
        previous = self._staged_get(staged_rows, schema, table, pk)
        if previous is None:
            if not self._is_complete(values, sources):
                raise UnresolvedRowStateError(
                    "cannot retract %s.%s row %r; its previous state is unknown "
                    "and the delete image is MINIMAL" % (schema, table, pk)
                )
            previous = self._build_state(
                schema, table, pk, self._present_values(values, sources)
            )
        self._retract(deltas, previous)
        self._staged_delete(staged_rows, schema, table, pk)

    def _build_state(self, schema, table, pk, values):
        if self._time_column not in values:
            raise InvalidBusinessTimeError(
                "business time column %r is missing from %s.%s row %r"
                % (self._time_column, schema, table, pk)
            )
        try:
            business_time = _as_utc(values[self._time_column], what="business time")
        except TypeError as exc:
            raise InvalidBusinessTimeError(str(exc))
        if self._amount_column not in values:
            raise InvalidAmountError(
                "amount column %r is missing from %s.%s row %r"
                % (self._amount_column, schema, table, pk)
            )
        amount = _validate_amount(values[self._amount_column])
        start, _end = self._window_bounds(business_time)
        group_key = _group_of(values, self._group_columns)
        return _RowState(
            schema,
            table,
            pk,
            dict(values),
            business_time,
            start,
            group_key,
            amount,
        )

    # -- staged row overlay ---------------------------------------------------
    @staticmethod
    def _row_key(schema, table, pk):
        return (schema, table, pk)

    def _staged_get(self, staged_rows, schema, table, pk):
        key = self._row_key(schema, table, pk)
        if key in staged_rows:
            return staged_rows[key]
        return self._rows.get(key)

    def _staged_set(self, staged_rows, schema, table, pk, state):
        staged_rows[self._row_key(schema, table, pk)] = state

    def _staged_delete(self, staged_rows, schema, table, pk):
        staged_rows[self._row_key(schema, table, pk)] = None

    # -- aggregate deltas -----------------------------------------------------
    @staticmethod
    def _delta_entry(deltas, window_id, group_key):
        groups = deltas.setdefault(window_id, {})
        entry = groups.get(group_key)
        if entry is None:
            entry = [0, 0, 0]
            groups[group_key] = entry
        return entry

    def _contribute(self, deltas, state):
        entry = self._delta_entry(deltas, state.window_id.isoformat(), state.group_key)
        entry[0] += 1
        if state.amount is not None:
            entry[1] += 1
            entry[2] += state.amount

    def _retract(self, deltas, state):
        entry = self._delta_entry(deltas, state.window_id.isoformat(), state.group_key)
        entry[0] -= 1
        if state.amount is not None:
            entry[1] -= 1
            entry[2] -= state.amount

    # -- publication ----------------------------------------------------------
    def _publish_window(self, window, candidates):
        """Update ``window`` in place, returning ``(group, count, sum)`` rows.

        A window's first publication carries the complete statistics of every
        group ever seen in it (empty groups included, as ``0``/``None``).  Later
        publications only contain the groups whose result actually changed, so
        an identical recomputation is never republished.
        """
        emitted = []
        first_publication = window.revision == 0
        for group_key in sorted(candidates, key=repr):
            result = window.result(group_key)
            if window.last_emitted.get(group_key) == result:
                continue
            if (
                not first_publication
                and result[0] == 0
                and group_key not in window.last_emitted
            ):
                continue
            emitted.append((group_key, result[0], result[1]))
        if emitted:
            window.revision += 1
            for group_key, count_all, sum_amount in emitted:
                window.last_emitted[group_key] = (count_all, sum_amount)
        return emitted

    def _stage_publication(self, deltas):
        """Compute candidate windows without mutating committed state."""
        publishable = []
        for window_id in sorted(deltas):
            groups = deltas[window_id]
            touched = {
                group_key
                for group_key, entry in groups.items()
                if entry != [0, 0, 0]
            }
            if not touched:
                continue
            candidate = self._window_for_id(window_id).clone()
            for group_key, (count_delta, non_null_delta, sum_delta) in groups.items():
                candidate.apply(group_key, count_delta, non_null_delta, sum_delta)
            if (
                not candidate.closed
                and self._watermark is not None
                and candidate.end <= self._watermark
            ):
                candidate.closed = True
            if not candidate.closed:
                publishable.append((window_id, candidate, []))
                continue
            if candidate.revision == 0:
                candidates = candidate.groups()
            else:
                candidates = touched
            emitted = self._publish_window(candidate, candidates)
            publishable.append((window_id, candidate, emitted))
        return publishable

    def _commit_publishable(
        self,
        publishable,
        watermark=None,
        transaction=None,
        checkpoint=None,
        staged_rows=None,
    ):
        """Publish ``publishable`` atomically.

        The report rows, the aggregates, the window sealing state, the report
        events and the source checkpoint all become visible together; nothing
        is mutated until every record has been built.
        """
        sequence = self._sequence
        new_records = []
        for window_id, window, emitted in publishable:
            for group_key, count_all, sum_amount in emitted:
                sequence += 1
                new_records.append(
                    WindowRecord(
                        sequence,
                        window_id,
                        window.start,
                        window.end,
                        group_key,
                        count_all,
                        sum_amount,
                        window.revision,
                    )
                )
        # Only assignments below: no validation can fail and leave a half
        # applied transaction behind.
        if staged_rows is not None:
            for key, state in staged_rows.items():
                if state is None:
                    self._rows.pop(key, None)
                else:
                    self._rows[key] = state
        for window_id, window, _emitted in publishable:
            self._windows[window_id] = window
        self._records.extend(new_records)
        self._sequence = sequence
        if watermark is not None:
            self._watermark = watermark
        if transaction is not None:
            self._remember_transaction(transaction, checkpoint)
        elif checkpoint is not None:
            self._last_checkpoint = checkpoint.key
            self._applied_checkpoints[checkpoint.key] = True
        return new_records

    # -- public queries -------------------------------------------------------
    @property
    def records(self):
        """Append-only published records in publication order."""
        with self._lock:
            return tuple(self._records)

    def read_records(self, after_sequence=0):
        """Return every record with ``sequence > after_sequence``.

        This is the cursor-readable event feed: records are append-only and
        ordered, so a consumer can persist the last seen ``sequence`` and reread
        the feed after a restart without gaps or duplicates.
        """
        with self._lock:
            return [r for r in self._records if r.sequence > after_sequence]

    def iter_records(self, after_sequence=0):
        return iter(self.read_records(after_sequence))

    # Cursor-readable feed aliases.
    records_after = read_records
    records_since = read_records
    read_reports = read_records

    def reports(self):
        """Alias of :attr:`records`."""
        return self.records

    def windows(self):
        """Return ``{window_id: (start, end, sealed)}`` for every window."""
        with self._lock:
            return {
                window_id: (window.start, window.end, window.closed)
                for window_id, window in sorted(self._windows.items())
            }

    def window_totals(self, window_id):
        """Return the live ``{group_key: (count_all, sum_amount)}`` of a window."""
        with self._lock:
            window = self._windows.get(window_id)
            if window is None:
                return {}
            return {
                group: window.result(group)
                for group in sorted(window.groups(), key=repr)
            }

    def published_totals(self, window_id):
        """Return the last published ``{group_key: (count, sum)}`` of a window."""
        with self._lock:
            window = self._windows.get(window_id)
            if window is None:
                return {}
            return dict(window.last_emitted)

    def get_row(self, schema, table, primary_key):
        """Return the last known values of a tracked row, or ``None``."""
        if not isinstance(primary_key, tuple):
            primary_key = (primary_key,)
        key = (schema, table, tuple(_hashable(v) for v in primary_key))
        with self._lock:
            state = self._rows.get(key)
            return None if state is None else dict(state.values)

    def rows(self):
        """Return ``{(schema, table, pk): values}`` for every tracked row."""
        with self._lock:
            return {
                key: dict(state.values) for key, state in self._rows.items()
            }

    @property
    def sealed_windows(self):
        with self._lock:
            return tuple(
                sorted(window_id for window_id, w in self._windows.items() if w.closed)
            )

    @property
    def open_windows(self):
        with self._lock:
            return tuple(
                sorted(
                    window_id for window_id, w in self._windows.items() if not w.closed
                )
            )

    # -- persistence ----------------------------------------------------------
    def dump(self):
        """Return a JSON-serialisable snapshot of the whole report."""
        with self._lock:
            return {
                "version": _STORE_VERSION,
                "config": {
                    "window_width_us": self._width_us(),
                    "window_origin": self._origin.isoformat(),
                    "time_column": self._time_column,
                    "amount_column": self._amount_column,
                    "group_columns": list(self._group_columns),
                },
                "sequence": self._sequence,
                "watermark": None
                if self._watermark is None
                else self._watermark.isoformat(),
                "checkpoint": None
                if self._last_checkpoint is None
                else list(self._last_checkpoint),
                "rows": [state.as_dict() for state in self._rows.values()],
                "windows": {
                    window_id: window.as_dict()
                    for window_id, window in self._windows.items()
                },
                "records": [record.as_dict() for record in self._records],
                "applied_transaction_ids": [
                    [
                        _encode(transaction_id),
                        None if checkpoint is None else list(checkpoint.key),
                    ]
                    for transaction_id, checkpoint in self._applied_ids.items()
                ],
                "applied_checkpoints": [
                    list(key) for key in self._applied_checkpoints
                ],
            }

    def save(self, path):
        """Atomically persist the report; the previous file is kept on failure."""
        with self._lock:
            data = self.dump()
        directory = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".window-report-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(data, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise

    def load(self, source):
        """Restore report state from a path or a snapshot from :meth:`dump`."""
        if isinstance(source, dict):
            data = source
        else:
            with open(source, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        self._restore(data)

    @classmethod
    def open(cls, path, **kwargs):
        """Build a report from a saved snapshot, reusing its configuration."""
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        config = data["config"]
        kwargs.setdefault(
            "window_width", datetime.timedelta(microseconds=config["window_width_us"])
        )
        kwargs.setdefault(
            "window_origin", datetime.datetime.fromisoformat(config["window_origin"])
        )
        for name in ("time_column", "amount_column"):
            kwargs.setdefault(name, config[name])
        kwargs.setdefault("group_columns", tuple(config["group_columns"]))
        report = cls(**kwargs)
        report._restore(data)
        return report

    def _restore(self, data):
        if data.get("version") != _STORE_VERSION:
            raise WindowReportError(
                "unsupported snapshot version %r" % (data.get("version"),)
            )
        config = data["config"]
        expected = {
            "window_width_us": self._width_us(),
            "window_origin": self._origin.isoformat(),
            "time_column": self._time_column,
            "amount_column": self._amount_column,
            "group_columns": list(self._group_columns),
        }
        if config != expected:
            raise WindowReportError(
                "snapshot configuration %r does not match this report %r"
                % (config, expected)
            )
        rows = {}
        for entry in data["rows"]:
            state = _RowState.from_dict(entry)
            rows[(state.schema, state.table, state.pk)] = state
        windows = {
            window_id: _WindowState.from_dict(window)
            for window_id, window in data["windows"].items()
        }
        records = [WindowRecord.from_dict(entry) for entry in data["records"]]
        applied = collections.OrderedDict()
        for encoded_id, checkpoint in data.get("applied_transaction_ids", []):
            applied[_decode(encoded_id)] = (
                None if checkpoint is None else Checkpoint(*checkpoint)
            )
        applied_checkpoints = collections.OrderedDict(
            (tuple(key), True)
            for key in data.get("applied_checkpoints", [])
        )
        with self._lock:
            self._rows = rows
            self._windows = windows
            self._records = records
            self._sequence = data["sequence"]
            self._watermark = (
                None
                if data["watermark"] is None
                else datetime.datetime.fromisoformat(data["watermark"])
            )
            self._last_checkpoint = (
                None if data["checkpoint"] is None else tuple(data["checkpoint"])
            )
            self._applied_ids = applied
            self._applied_checkpoints = applied_checkpoints


# Backwards-friendly aliases for the reporting entry point.
WindowReport = CorrectableWindowReport
CDCWindowReport = CorrectableWindowReport
