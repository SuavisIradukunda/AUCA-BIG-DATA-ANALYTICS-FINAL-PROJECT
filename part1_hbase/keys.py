"""Row-key design helpers shared by the HBase loader, queries and integration code."""
import calendar
import datetime

MAX_TS = 9_999_999_999
SESSION_FAMILIES = {"s": dict(max_versions=1), "pv": dict(max_versions=1), "c": dict(max_versions=1)}
METRIC_FAMILIES = {"m": dict(max_versions=1)}


def epoch(ts):
    """ISO string or datetime -> epoch seconds (timestamps are treated as UTC)."""
    if isinstance(ts, str):
        ts = datetime.datetime.fromisoformat(ts)
    return calendar.timegm(ts.timetuple())


def reverse_ts(ts):
    return f"{MAX_TS - epoch(ts):010d}"


def session_row_key(user_id, start_time, session_id):
    """<user_id>#<reverse_ts>#<session_id>: user's sessions contiguous, newest first."""
    return f"{user_id}#{reverse_ts(start_time)}#{session_id}".encode()


def user_time_range(user_id, start, end):
    """(row_start, row_stop) covering a user's sessions with start <= time < end.

    Because the timestamp is reversed, the scan starts at the END of the window.
    """
    return (f"{user_id}#{MAX_TS - epoch(end) + 1:010d}".encode(),
            f"{user_id}#{MAX_TS - epoch(start) + 1:010d}".encode())


def metric_row_key(product_id, day):
    """<product_id>#<yyyymmdd>"""
    if not isinstance(day, str):
        day = day.strftime("%Y%m%d")
    return f"{product_id}#{day.replace('-', '')[:8]}".encode()
