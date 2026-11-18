############################
Correctable CDC window reports
############################

``pymysqlreplication.window_report`` adds a reporting layer on top of the
native row events.  The existing :class:`~pymysqlreplication.BinLogStreamReader`
API is unchanged; the report is a pure consumer of the events.

The report consumes **whole transactions** of native row events together with
the trailing ``file``/``pos`` coordinate of each transaction.  It maintains
windowed aggregates that stay correct when late, re-classifying or replayed
changes arrive.

Semantics
=========

Business time
    Each row carries a business timestamp from ``time_column``.  It is
    interpreted as UTC: naive datetimes are assumed to be UTC and aware ones are
    converted.  The commit timestamp of the event is never used instead.

Windows
    Windows have a fixed width, are aligned on ``window_origin`` and are half
    open (``[start, end)``).  ``window_id`` is the ISO-8601 UTC representation
    of the window start.

Aggregates
    For every ``(window_id, group_key)`` pair the report keeps ``count_all``
    (rows, including ``NULL`` amounts) and the exact integer sum of the
    non-``NULL`` amounts (``None`` when there is no non-``NULL`` amount).

MINIMAL row images
    Row identity comes from the primary key declared by the table map.
    ``binlog_row_image=MINIMAL`` updates (primary key in the before image,
    changed columns in the after image) and deletes (primary key only) are
    completed from the tracked state of the row.

Corrections
    Moving a row to another group, moving it to another business time and
    deleting it retract the contribution from the original window/group before
    the new contribution is added.

Watermark
    Windows are sealed by an explicitly supplied, monotonically increasing
    watermark; the commit time is never used.  A window is sealed once its end
    is at or before the watermark.  Open windows are not published early.

Revisions
    Sealing a window publishes its complete statistics with revision ``1``.  A
    late change publishes the changed groups for the same ``window_id`` with an
    incremented revision.  An identical recomputation is not republished, and a
    group that becomes empty is republished as ``count_all=0`` and a ``None``
    sum.

Transactions, replay and durability
    Only the final state of each source transaction is published.  Facts,
    aggregates, window state, report events and the ``(file, pos)`` checkpoint
    are committed together; on failure the previous state is kept.  Replayed
    transactions (same coordinate or transaction id) and reopened stores never
    apply a correction twice.

Example
=======

.. code-block:: python

    import datetime
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.window_report import (
        CorrectableWindowReport,
        Transaction,
    )

    report = CorrectableWindowReport(
        window_width=datetime.timedelta(hours=1),
        window_origin=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        time_column="created_at",
        amount_column="amount",
        group_columns=("region",),
    )

    stream = BinLogStreamReader(...)
    # The application is responsible for splitting complete transactions and
    # for supplying the trailing coordinate of each one.
    for transaction in split_transactions(stream):
        report.accept(transaction)          # publishes only final state
        for record in report.records_after(last_sequence):
            publish(record.as_dict())
            last_sequence = record.sequence

    # An explicit watermark seals the finished windows.
    report.advance_watermark(datetime.datetime.now(datetime.timezone.utc))
    for record in report.records_after(last_sequence):
        publish(record.as_dict())
        last_sequence = record.sequence

API
===

.. autoclass:: pymysqlreplication.window_report.CorrectableWindowReport
    :members: accept, advance_watermark, read_records, window_totals,
              published_totals, get_row, save, load, open, dump

.. autoclass:: pymysqlreplication.window_report.Transaction

.. autoclass:: pymysqlreplication.window_report.Checkpoint

.. autoclass:: pymysqlreplication.window_report.WindowRecord
    :members: as_dict, from_dict, windowID, groupKey, sum

.. autoexception:: pymysqlreplication.window_report.WindowReportError
.. autoexception:: pymysqlreplication.window_report.WatermarkRegressionError
.. autoexception:: pymysqlreplication.window_report.MissingPrimaryKeyError
.. autoexception:: pymysqlreplication.window_report.UnresolvedRowStateError
.. autoexception:: pymysqlreplication.window_report.InvalidAmountError
.. autoexception:: pymysqlreplication.window_report.InvalidBusinessTimeError
