"""Unit tests for :mod:`pymysqlreplication.window_report`.

These tests do not need a MySQL server: they feed native-shaped row events
(the same dictionaries produced by ``RowsEvent.rows``) to the report.
"""

import datetime
import unittest

from pymysqlreplication.constants import BINLOG, NONE_SOURCE
from pymysqlreplication.window_report import (
    Checkpoint,
    CorrectableWindowReport,
    InvalidAmountError,
    InvalidBusinessTimeError,
    MissingPrimaryKeyError,
    Transaction,
    UnresolvedRowStateError,
    WatermarkRegressionError,
    WindowRecord,
)

UTC = datetime.timezone.utc
DAY = datetime.date(2026, 1, 1)


def ts(hour, minute=0):
    return datetime.datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


FULL_COLUMNS = ("id", "ts", "amount", "grp")


class FakeRowEvent:
    """Minimal stand-in for a native Write/Update/DeleteRowsEvent."""

    def __init__(self, schema, table, primary_key, rows, event_type=None):
        self.schema = schema
        self.table = table
        self.primary_key = primary_key
        self.rows = rows
        if event_type is not None:
            self.event_type = event_type

    def _dump(self):
        pass


def write_event(*rows, schema="app", table="orders", primary_key="id"):
    payload = []
    for row in rows:
        values = dict(zip(FULL_COLUMNS, row))
        payload.append({"values": values, "none_sources": {}})
    return FakeRowEvent(
        schema, table, primary_key, payload, BINLOG.WRITE_ROWS_EVENT_V2
    )


def update_event(old, new, *, primary_key="id", schema="app", table="orders"):
    """Build an update event. ``old``/``new`` are dicts of present columns."""
    payload = [
        {
            "before_values": dict(old),
            "before_none_sources": {
                column: NONE_SOURCE.COLS_BITMAP
                for column in FULL_COLUMNS
                if column not in old
            },
            "after_values": dict(new),
            "after_none_sources": {
                column: NONE_SOURCE.COLS_BITMAP
                for column in FULL_COLUMNS
                if column not in new
            },
        }
    ]
    return FakeRowEvent(
        schema, table, primary_key, payload, BINLOG.UPDATE_ROWS_EVENT_V2
    )


def delete_event(old, *, primary_key="id", schema="app", table="orders"):
    payload = [
        {
            "values": dict(old),
            "none_sources": {
                column: NONE_SOURCE.COLS_BITMAP
                for column in FULL_COLUMNS
                if column not in old
            },
        }
    ]
    return FakeRowEvent(
        schema, table, primary_key, payload, BINLOG.DELETE_ROWS_EVENT_V2
    )


def make_report(width=3600, **kwargs):
    kwargs.setdefault("time_column", "ts")
    kwargs.setdefault("amount_column", "amount")
    kwargs.setdefault("group_columns", ("grp",))
    kwargs.setdefault("window_origin", datetime.datetime(2026, 1, 1, tzinfo=UTC))
    return CorrectableWindowReport(datetime.timedelta(seconds=width), **kwargs)


def wid(hour):
    return "2026-01-01T%02d:00:00+00:00" % hour


class TestWindowBoundaries(unittest.TestCase):
    def test_fixed_width_and_origin(self):
        report = make_report(width=3600)
        start, end = report._window_bounds(ts(5, 59))
        self.assertEqual(start, ts(5))
        self.assertEqual(end, ts(6))
        # Exactly on a boundary belongs to the following window.
        start, _end = report._window_bounds(ts(6))
        self.assertEqual(start, ts(6))

    def test_unaligned_origin(self):
        report = make_report(
            width=600,
            window_origin=datetime.datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
        )
        start, end = report._window_bounds(ts(0, 17))
        self.assertEqual(start, datetime.datetime(2026, 1, 1, 0, 15, tzinfo=UTC))
        self.assertEqual(end, datetime.datetime(2026, 1, 1, 0, 25, tzinfo=UTC))

    def test_naive_datetime_is_treated_as_utc(self):
        report = make_report()
        start, _end = report._window_bounds(datetime.datetime(2026, 1, 1, 3, 30))
        self.assertEqual(start, ts(3))

    def test_aware_time_is_converted_to_utc(self):
        report = make_report()
        tz = datetime.timezone(datetime.timedelta(hours=8))
        start, _end = report._window_bounds(
            datetime.datetime(2026, 1, 1, 11, 30, tzinfo=tz)
        )
        self.assertEqual(start, ts(3))


class TestBasicAggregation(unittest.TestCase):
    def test_counts_and_exact_nullable_sum(self):
        report = make_report()
        report.accept(
            Transaction(
                [
                    write_event(
                        (1, ts(1), 10, "a"),
                        (2, ts(1), None, "a"),
                        (3, ts(1), -7, "a"),
                        (4, ts(1), 5, "b"),
                    )
                ],
                log_file="bin.000001",
                log_pos=100,
            )
        )
        self.assertEqual(report.window_totals(wid(1))[("a",)], (3, 3))
        self.assertEqual(report.window_totals(wid(1))[("b",)], (1, 5))

    def test_null_only_group_has_null_sum(self):
        report = make_report()
        report.accept([write_event((1, ts(1), None, "a"))])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, None))

    def test_float_amount_is_rejected(self):
        report = make_report()
        with self.assertRaises(InvalidAmountError):
            report.accept([write_event((1, ts(1), 1.5, "a"))])
        # Failed transaction leaves no trace.
        self.assertEqual(report.records, ())
        self.assertIsNone(report.checkpoint)
        self.assertEqual(report.window_totals(wid(1)), {})

    def test_missing_time_column_is_rejected(self):
        report = make_report()
        event = FakeRowEvent(
            "app",
            "orders",
            "id",
            [{"values": {"id": 1, "amount": 2, "grp": "a"}, "none_sources": {}}],
            BINLOG.WRITE_ROWS_EVENT_V2,
        )
        with self.assertRaises(InvalidBusinessTimeError):
            report.accept([event])

    def test_missing_primary_key_is_rejected(self):
        report = make_report()
        event = write_event((1, ts(1), 1, "a"), primary_key="")
        with self.assertRaises(MissingPrimaryKeyError):
            report.accept([event])


class TestWatermarkPublication(unittest.TestCase):
    def test_open_window_not_published(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        self.assertEqual(report.records, ())
        self.assertEqual(report.open_windows, (wid(1),))

    def test_first_seal_publishes_complete_statistics(self):
        report = make_report()
        report.accept(
            [
                write_event(
                    (1, ts(1), 10, "a"),
                    (2, ts(1), None, "a"),
                    (3, ts(1), 4, "b"),
                )
            ]
        )
        emitted = report.advance_watermark(ts(2))
        self.assertEqual(len(emitted), 2)
        self.assertTrue(all(record.revision == 1 for record in emitted))
        self.assertTrue(all(record.window_id == wid(1) for record in emitted))
        self.assertEqual(
            report.published_totals(wid(1)),
            {("a",): (2, 10), ("b",): (1, 4)},
        )

    def test_seal_boundary_is_inclusive_of_window_end(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        # Window [01:00, 02:00) is sealed when the watermark reaches 02:00.
        self.assertEqual(report.advance_watermark(ts(2) - datetime.timedelta(microseconds=1)), [])
        self.assertEqual(len(report.advance_watermark(ts(2))), 1)

    def test_watermark_does_not_regress(self):
        report = make_report()
        report.advance_watermark(ts(5))
        with self.assertRaises(WatermarkRegressionError):
            report.advance_watermark(ts(4))
        self.assertEqual(report.watermark, ts(5))

    def test_watermark_ignores_commit_timestamps(self):
        report = make_report()
        # Event timestamp (commit time) is irrelevant; only the business ts is.
        event = write_event((1, ts(1), 10, "a"))
        event.timestamp = 9999999999
        report.accept([event])
        self.assertEqual(report.open_windows, (wid(1),))
        report.advance_watermark(ts(2))
        self.assertEqual(report.published_totals(wid(1)), {("a",): (1, 10)})


class TestCorrections(unittest.TestCase):
    def test_late_insert_corrects_sealed_window_with_new_revision(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        self.assertEqual(report.published_totals(wid(1)), {("a",): (1, 10)})

        emitted = report.accept(
            [write_event((2, ts(1), 5, "a"))], log_file="b.1", log_pos=20
        )
        self.assertEqual(len(emitted), 1)
        record = emitted[0]
        self.assertEqual(record.window_id, wid(1))
        self.assertEqual(record.group_key, ("a",))
        self.assertEqual(record.count_all, 2)
        self.assertEqual(record.sum_amount, 15)
        self.assertEqual(record.revision, 2)

    def test_identical_recomputation_is_not_republished(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        before = report.records
        # A donor move that nets out: a -> a with the same amount.
        emitted = report.accept(
            [
                update_event({"id": 1, "grp": "a"}, {"id": 1, "grp": "b"}),
                update_event({"id": 1, "grp": "b"}, {"id": 1, "grp": "a"}),
            ],
            log_file="b.1",
            log_pos=20,
        )
        self.assertEqual(emitted, [])
        self.assertEqual(report.records, before)

    def test_move_between_groups_corrects_original_window(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        emitted = report.accept(
            [update_event({"id": 1, "grp": "a"}, {"id": 1, "grp": "b"})],
            log_file="b.1",
            log_pos=20,
        )
        by_group = {record.group_key: record for record in emitted}
        self.assertEqual(by_group[("a",)].count_all, 0)
        self.assertEqual(by_group[("a",)].sum_amount, None)
        self.assertEqual(by_group[("b",)].count_all, 1)
        self.assertEqual(by_group[("b",)].sum_amount, 10)
        self.assertEqual(by_group[("a",)].revision, 2)

    def test_move_between_windows_corrects_original_window(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(3))
        emitted = report.accept(
            [update_event({"id": 1}, {"id": 1, "ts": ts(2)})],
            log_file="b.1",
            log_pos=20,
        )
        by_window = {record.window_id: record for record in emitted}
        self.assertEqual(by_window[wid(1)].count_all, 0)
        self.assertEqual(by_window[wid(1)].sum_amount, None)
        self.assertEqual(by_window[wid(2)].count_all, 1)
        self.assertEqual(by_window[wid(2)].sum_amount, 10)

    def test_delete_corrects_original_window(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        emitted = report.accept(
            [delete_event({"id": 1})], log_file="b.1", log_pos=20
        )
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].count_all, 0)
        self.assertEqual(emitted[0].sum_amount, None)
        self.assertEqual(emitted[0].revision, 2)
        self.assertEqual(report.get_row("app", "orders", (1,)), None)

    def test_empty_window_still_reports_zero_and_null(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        report.accept([delete_event({"id": 1})], log_file="b.1", log_pos=20)
        self.assertEqual(report.published_totals(wid(1)), {("a",): (0, None)})
        self.assertEqual(report.window_totals(wid(1)), {("a",): (0, None)})


class TestMinimalRowImages(unittest.TestCase):
    def test_minimal_update_uses_tracked_state(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        # MINIMAL update only carries the PK in the before image.
        report.accept([update_event({"id": 1}, {"amount": 4})])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 4))
        row = report.get_row("app", "orders", (1,))
        self.assertEqual(row["ts"], ts(1))
        self.assertEqual(row["grp"], "a")

    def test_minimal_update_moves_group_using_tracked_state(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.accept([update_event({"id": 1}, {"grp": "b"})])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (0, None))
        self.assertEqual(report.window_totals(wid(1))[("b",)], (1, 10))

    def test_minimal_update_moves_window_using_tracked_state(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.accept([update_event({"id": 1}, {"ts": ts(2)})])
        self.assertEqual(report.window_totals(wid(1)).get(("a",), (0, None)), (0, None))
        self.assertEqual(report.window_totals(wid(2))[("a",)], (1, 10))

    def test_minimal_delete_uses_tracked_state(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.accept([delete_event({"id": 1})])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (0, None))

    def test_unknown_minimal_delete_is_an_error_and_changes_nothing(self):
        report = make_report()
        with self.assertRaises(UnresolvedRowStateError):
            report.accept([delete_event({"id": 99})])
        self.assertEqual(report.records, ())
        self.assertEqual(report.window_totals(wid(1)), {})

    def test_composite_declared_primary_key(self):
        report = make_report()

        def composite_write(a, b, amount):
            event = write_event((a, ts(1), amount, "a"))
            event.primary_key = ("id", "grp")
            return event

        report.accept([composite_write(1, "a", 10)])
        # The declared composite key is used for row identity.
        self.assertIsNotNone(report.get_row("app", "orders", (1, "a")))
        self.assertIsNone(report.get_row("app", "orders", (1,)))
        # An update addressed by the full composite key works with MINIMAL.
        event = update_event({"id": 1, "grp": "a"}, {"amount": 3})
        event.primary_key = ("id", "grp")
        report.accept([event])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 3))


class TestTransactionAtomicity(unittest.TestCase):
    def test_only_final_state_of_transaction_is_published(self):
        report = make_report()
        report.advance_watermark(ts(5))
        emitted = report.accept(
            [
                write_event((1, ts(1), 100, "a")),
                update_event({"id": 1}, {"amount": 200}),
                update_event({"id": 1}, {"amount": 300}),
            ],
            log_file="b.1",
            log_pos=50,
        )
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].sum_amount, 300)
        self.assertEqual(emitted[0].revision, 1)

    def test_failure_keeps_previous_state(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        records_before = report.records
        with self.assertRaises(InvalidAmountError):
            report.accept(
                [
                    write_event((2, ts(1), 5, "a")),
                    write_event((3, ts(1), "nope", "a")),
                ],
                log_file="b.1",
                log_pos=20,
            )
        self.assertEqual(report.records, records_before)
        self.assertEqual(report.published_totals(wid(1)), {("a",): (1, 10)})
        self.assertEqual(report.checkpoint, Checkpoint("b.1", 10))
        self.assertIsNone(report.get_row("app", "orders", (2,)))


class TestReplayAndReopen(unittest.TestCase):
    def _seed(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        report.advance_watermark(ts(2))
        return report

    def test_replayed_coordinate_is_not_applied_twice(self):
        report = self._seed()
        before = report.records
        emitted = report.accept(
            [write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10
        )
        self.assertEqual(emitted, [])
        self.assertEqual(report.records, before)
        self.assertEqual(report.published_totals(wid(1)), {("a",): (1, 10)})

    def test_replayed_transaction_id_is_not_applied_twice(self):
        report = make_report()
        transaction = Transaction(
            [write_event((1, ts(1), 10, "a"))], transaction_id="gtid-1"
        )
        report.accept(transaction)
        # The same transaction object is a replay.
        self.assertEqual(report.accept(transaction), [])
        # Same id with a different coordinate is still a replay.
        replay = Transaction(
            [write_event((1, ts(1), 10, "a"))],
            log_file="b.1",
            log_pos=999,
            transaction_id="gtid-1",
        )
        self.assertEqual(report.accept(replay), [])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 10))

    def test_snapshot_roundtrip_preserves_cursor_and_state(self):
        report = self._seed()
        path = "/tmp/window-report-test.json"
        report.save(path)
        reopened = CorrectableWindowReport.open(path)
        self.assertEqual(reopened.records, report.records)
        self.assertEqual(reopened.watermark, ts(2))
        self.assertEqual(reopened.checkpoint, Checkpoint("b.1", 10))
        self.assertEqual(reopened.published_totals(wid(1)), {("a",): (1, 10)})
        self.assertEqual(reopened.get_row("app", "orders", (1,))["amount"], 10)

        # Replaying an already applied transaction after reopen is a no-op.
        self.assertEqual(
            reopened.accept(
                [write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10
            ),
            [],
        )
        # A genuinely late change still corrects with the next revision.
        emitted = reopened.accept(
            [write_event((2, ts(1), 1, "a"))], log_file="b.1", log_pos=20
        )
        self.assertEqual(emitted[0].revision, 2)
        self.assertEqual(emitted[0].count_all, 2)

    def test_cursor_reread_has_no_gaps_or_duplicates(self):
        report = make_report()
        report.advance_watermark(ts(5))
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10)
        first = report.read_records(0)
        self.assertEqual(len(first), 1)
        cursor = first[-1].sequence
        report.accept([write_event((2, ts(2), 3, "b"))], log_file="b.1", log_pos=20)
        later = report.read_records(cursor)
        self.assertEqual(len(later), 1)
        self.assertEqual(later[0].group_key, ("b",))
        self.assertEqual(
            [record.sequence for record in report.read_records(0)],
            [1, 2],
        )


class TestRecordShape(unittest.TestCase):
    def test_record_is_json_serialisable_and_ordered(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        record = report.advance_watermark(ts(2))[0]
        data = record.as_dict()
        self.assertEqual(data["window_id"], wid(1))
        self.assertEqual(data["group_key"], ["a"])
        self.assertEqual(data["count_all"], 1)
        self.assertEqual(data["sum_amount"], 10)
        self.assertEqual(data["revision"], 1)
        self.assertEqual(WindowRecord.from_dict(data), record)

    def test_empty_group_key_when_no_group_columns(self):
        report = CorrectableWindowReport(
            datetime.timedelta(hours=1),
            window_origin=datetime.datetime(2026, 1, 1, tzinfo=UTC),
            time_column="ts",
            amount_column="amount",
        )
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.advance_watermark(ts(2))
        self.assertEqual(report.published_totals(wid(1)), {(): (1, 10)})


if __name__ == "__main__":
    unittest.main()


class TestAdditionalSemantics(unittest.TestCase):
    def test_duplicate_insert_overwrites_instead_of_double_counting(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.accept([write_event((1, ts(1), 7, "a"))])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 7))

    def test_update_that_changes_primary_key_moves_tracked_row(self):
        report = make_report()

        def event(before, after):
            payload = [
                {
                    "before_values": before,
                    "before_none_sources": {},
                    "after_values": after,
                    "after_none_sources": {},
                }
            ]
            return FakeRowEvent(
                "app", "orders", "id", payload, BINLOG.UPDATE_ROWS_EVENT_V2
            )

        report.accept([write_event((1, ts(1), 10, "a"))])
        report.accept(
            [
                event(
                    {"id": 1, "ts": ts(1), "amount": 10, "grp": "a"},
                    {"id": 2, "ts": ts(1), "amount": 10, "grp": "a"},
                )
            ]
        )
        self.assertIsNone(report.get_row("app", "orders", (1,)))
        self.assertIsNotNone(report.get_row("app", "orders", (2,)))
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 10))

    def test_no_group_columns_yields_empty_group_key(self):
        report = CorrectableWindowReport(
            datetime.timedelta(hours=1),
            window_origin=datetime.datetime(2026, 1, 1, tzinfo=UTC),
            time_column="ts",
            amount_column="amount",
        )
        report.accept([write_event((1, ts(1), 10, "a"), (2, ts(1), None, "b"))])
        self.assertEqual(report.window_totals(wid(1)), {(): (2, 10)})

    def test_decimal_integral_amount_is_exact(self):
        import decimal

        report = make_report()
        report.accept([write_event((1, ts(1), decimal.Decimal("12"), "a"))])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 12))
        with self.assertRaises(InvalidAmountError):
            report.accept([write_event((2, ts(1), decimal.Decimal("1.5"), "a"))])

    def test_resume_from_older_coordinate_is_replay(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=50)
        self.assertEqual(
            report.accept([write_event((2, ts(1), 1, "a"))], log_file="b.1", log_pos=40),
            [],
        )
        # A newer coordinate is applied normally.
        self.assertEqual(
            len(report.accept([write_event((2, ts(1), 1, "a"))], log_file="b.1", log_pos=60)),
            0,
        )
        self.assertEqual(report.window_totals(wid(1))[("a",)], (2, 11))

    def test_watermark_persists_across_reopen(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.advance_watermark(ts(2))
        path = "/tmp/window-report-watermark.json"
        report.save(path)
        reopened = CorrectableWindowReport.open(path)
        # Late change after reopen corrects the sealed window.
        emitted = reopened.accept([write_event((2, ts(1), 1, "a"))])
        self.assertEqual(emitted[0].revision, 2)
        self.assertEqual(reopened.published_totals(wid(1)), {("a",): (2, 11)})

    def test_save_is_atomic_on_failure(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        path = "/tmp/window-report-atomic.json"
        report.save(path)
        good = open(path).read()

        class Exploding:
            pass

        original = report.dump
        report.dump = lambda: {"version": 1, "config": Exploding()}
        with self.assertRaises(TypeError):
            report.save(path)
        report.dump = original
        self.assertEqual(open(path).read(), good)

    def test_snapshot_rejects_mismatched_configuration(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        snapshot = report.dump()
        other = make_report(width=1800)
        with self.assertRaises(Exception):
            other.load(snapshot)

    def test_records_are_immutable_and_ordered(self):
        report = make_report()
        report.advance_watermark(ts(5))
        report.accept(
            [write_event((1, ts(1), 10, "a"), (2, ts(1), 4, "b"))],
            log_file="b.1",
            log_pos=10,
        )
        report.accept(
            [write_event((3, ts(2), 4, "b"))], log_file="b.1", log_pos=20
        )
        sequences = [record.sequence for record in report.records]
        self.assertEqual(sequences, sorted(sequences))
        self.assertEqual(len(sequences), len(set(sequences)))
        # Every record is for a sealed window, never an open one.
        for record in report.records:
            self.assertIn(record.window_id, report.sealed_windows)


class TestDuckTypedEvents(unittest.TestCase):
    def test_events_without_event_type(self):
        report = make_report()

        class Write:
            schema = "app"
            table = "orders"
            primary_key = "id"
            rows = [
                {
                    "values": {
                        "id": 1,
                        "ts": ts(1),
                        "amount": 10,
                        "grp": "a",
                    },
                    "none_sources": {},
                }
            ]

        class Update:
            schema = "app"
            table = "orders"
            primary_key = "id"
            rows = [
                {
                    "before_values": {"id": 1},
                    "before_none_sources": {
                        "ts": NONE_SOURCE.COLS_BITMAP,
                        "amount": NONE_SOURCE.COLS_BITMAP,
                        "grp": NONE_SOURCE.COLS_BITMAP,
                    },
                    "after_values": {"grp": "b"},
                    "after_none_sources": {
                        "id": NONE_SOURCE.COLS_BITMAP,
                        "ts": NONE_SOURCE.COLS_BITMAP,
                        "amount": NONE_SOURCE.COLS_BITMAP,
                    },
                }
            ]

        report.accept([Write()])
        report.accept([Update()])
        self.assertEqual(report.window_totals(wid(1))[("a",)], (0, None))
        self.assertEqual(report.window_totals(wid(1))[("b",)], (1, 10))

    def test_duck_typed_transaction_object(self):
        report = make_report()

        class SourceTransaction:
            events = [write_event((1, ts(1), 10, "a"))]
            log_file = "b.1"
            log_pos = 10
            transaction_id = "x-1"

        report.accept(SourceTransaction())
        self.assertEqual(report.checkpoint, Checkpoint("b.1", 10))
        self.assertEqual(report.window_totals(wid(1))[("a",)], (1, 10))


class TestSealEdgeCases(unittest.TestCase):
    def test_window_emptied_before_first_seal_still_reports_zero(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        report.accept([delete_event({"id": 1})])
        emitted = report.advance_watermark(ts(2))
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].window_id, wid(1))
        self.assertEqual(emitted[0].count_all, 0)
        self.assertEqual(emitted[0].sum_amount, None)
        self.assertEqual(emitted[0].revision, 1)

    def test_late_window_beyond_existing_watermark_is_sealed_by_ingest(self):
        report = make_report()
        report.advance_watermark(ts(2))
        emitted = report.accept(
            [write_event((1, ts(1), 10, "a"))], log_file="b.1", log_pos=10
        )
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].revision, 1)
        self.assertIn(wid(1), report.sealed_windows)

    def test_late_window_before_watermark_stays_open(self):
        report = make_report()
        report.advance_watermark(ts(1))
        emitted = report.accept([write_event((1, ts(1), 10, "a"))])
        self.assertEqual(emitted, [])
        self.assertIn(wid(1), report.open_windows)

    def test_repeated_watermark_does_not_republish(self):
        report = make_report()
        report.accept([write_event((1, ts(1), 10, "a"))])
        first = report.advance_watermark(ts(2))
        self.assertEqual(len(first), 1)
        self.assertEqual(report.advance_watermark(ts(2)), [])
        self.assertEqual(report.advance_watermark(ts(2)), [])
        self.assertEqual(len(report.records), 1)

    def test_multiple_group_corrections_share_revision(self):
        report = make_report()
        report.accept(
            [write_event((1, ts(1), 10, "a"), (2, ts(1), 10, "b"))],
            log_file="b.1",
            log_pos=10,
        )
        report.advance_watermark(ts(2))
        emitted = report.accept(
            [
                update_event({"id": 1}, {"amount": 1}),
                update_event({"id": 2}, {"amount": 2}),
            ],
            log_file="b.1",
            log_pos=20,
        )
        self.assertEqual(len(emitted), 2)
        self.assertEqual({record.revision for record in emitted}, {2})
        self.assertEqual(report.published_totals(wid(1)), {("a",): (1, 1), ("b",): (1, 2)})
