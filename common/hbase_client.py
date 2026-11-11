"""HBase connection factory.

Production path: `happybase` over the HBase Thrift server (start HBase with
`docker compose up hbase`, Thrift listens on port 9090).

Fallback path: `LocalHBaseEmulator`, a small single-file stand-in that implements
the subset of the happybase API used in this project (create_table, put, batch,
row, rows, scan with row_start / row_stop / row_prefix / columns / limit, counter_inc).
It keeps HBase's data model: rows sorted lexicographically by row key, cells
addressed as b"family:qualifier", bytes in and bytes out. It exists only so the
project can be tested on a machine without HBase; it is NOT a replacement for
HBase (no regions, no distribution, no versions beyond the latest).
"""
import socket
import sqlite3

from common import config


def _thrift_reachable(host, port, timeout=1.5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_connection(force_emulator=False):
    """Return a happybase.Connection, or the local emulator if HBase is unavailable."""
    if not force_emulator and _thrift_reachable(config.HBASE_HOST, config.HBASE_PORT):
        import happybase
        print(f"[hbase] Connected to HBase Thrift at {config.HBASE_HOST}:{config.HBASE_PORT}")
        return happybase.Connection(config.HBASE_HOST, config.HBASE_PORT, autoconnect=True)
    print(f"[hbase] HBase Thrift not reachable - using local emulator ({config.HBASE_EMULATOR_PATH})")
    return LocalHBaseEmulator(config.HBASE_EMULATOR_PATH)


# --------------------------------------------------------------------------- #
#  Local emulator (testing only)                                              #
# --------------------------------------------------------------------------- #
def _b(x):
    return x if isinstance(x, bytes) else str(x).encode()


class LocalHBaseEmulator:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("CREATE TABLE IF NOT EXISTS _tables (name TEXT PRIMARY KEY, families TEXT)")

    # -- admin ---------------------------------------------------------------
    def tables(self):
        return [r[0].encode() for r in self.db.execute("SELECT name FROM _tables")]

    def create_table(self, name, families):
        self.db.execute("INSERT INTO _tables VALUES (?, ?)", (name, ",".join(families)))
        self.db.execute(f'CREATE TABLE "t_{name}" (rk BLOB, col BLOB, val BLOB, '
                        f'PRIMARY KEY (rk, col)) WITHOUT ROWID')
        self.db.commit()

    def delete_table(self, name, disable=True):
        self.db.execute("DELETE FROM _tables WHERE name = ?", (name,))
        self.db.execute(f'DROP TABLE IF EXISTS "t_{name}"')
        self.db.commit()

    def table(self, name):
        name = name.decode() if isinstance(name, bytes) else name
        row = self.db.execute("SELECT families FROM _tables WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise KeyError(f"Table {name} does not exist")
        return _EmulatedTable(self.db, name, row[0].split(","))

    def close(self):
        self.db.commit()
        self.db.close()


class _EmulatedTable:
    def __init__(self, db, name, families):
        self.db, self.name, self.families = db, name, set(families)
        self.t = f'"t_{name}"'

    def _check(self, col):
        fam = col.split(b":", 1)[0].decode()
        if fam not in self.families:
            raise ValueError(f"Unknown column family '{fam}' in table {self.name}")

    def put(self, row, data):
        rows = []
        for col, val in data.items():
            col = _b(col)
            self._check(col)
            rows.append((_b(row), col, _b(val)))
        self.db.executemany(f"INSERT OR REPLACE INTO {self.t} VALUES (?, ?, ?)", rows)

    def batch(self, batch_size=None):
        return _Batch(self)

    def counter_inc(self, row, column, value=1):
        cur = self.row(row, columns=[column]).get(_b(column))
        new = (int.from_bytes(cur, "big", signed=True) if cur else 0) + value
        self.put(row, {column: new.to_bytes(8, "big", signed=True)})
        return new

    def row(self, row, columns=None):
        for _, data in self.scan(row_start=_b(row), row_stop=_b(row) + b"\x00", columns=columns):
            return data
        return {}

    def rows(self, rows, columns=None):
        out = []
        for r in rows:
            data = self.row(r, columns)
            if data:
                out.append((_b(r), data))
        return out

    def scan(self, row_start=None, row_stop=None, row_prefix=None, columns=None, limit=None):
        if row_prefix is not None:
            row_start = _b(row_prefix)
            row_stop = _b(row_prefix)[:-1] + bytes([_b(row_prefix)[-1] + 1])
        sql, args = f"SELECT rk, col, val FROM {self.t} WHERE 1=1", []
        if row_start is not None:
            sql += " AND rk >= ?"; args.append(_b(row_start))
        if row_stop is not None:
            sql += " AND rk < ?"; args.append(_b(row_stop))
        sql += " ORDER BY rk, col"
        wanted = [_b(c) for c in columns] if columns else None

        current_key, current, emitted = None, {}, 0
        for rk, col, val in self.db.execute(sql, args):
            if wanted and not any(col == w or (b":" not in w and col.startswith(w + b":"))
                                  for w in wanted):
                continue
            if rk != current_key:
                if current:
                    yield current_key, current
                    emitted += 1
                    if limit and emitted >= limit:
                        return
                current_key, current = rk, {}
            current[col] = val
        if current and not (limit and emitted >= limit):
            yield current_key, current


class _Batch:
    def __init__(self, table):
        self.table = table

    def put(self, row, data):
        self.table.put(row, data)

    def send(self):
        self.table.db.commit()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.send()
