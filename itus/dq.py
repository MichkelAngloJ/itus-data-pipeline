"""Collects every data problem the pipeline runs into, then writes data_quality_log."""


class DQLog:
    def __init__(self):
        self.entries = []

    def add(self, code, severity, message, *, layer="staging", source_file=None,
            row_number=None, company_id=None, period=None, field=None,
            observed=None, action=None):
        if action is None:
            action = {"error": "quarantined", "warn": "loaded_with_warning"}.get(severity, "logged")
        self.entries.append((
            code, severity, layer, source_file, row_number, company_id, period,
            field, None if observed is None else str(observed), message, action,
        ))

    def write(self, conn):
        with conn:
            conn.execute("DROP TABLE IF EXISTS data_quality_log")
            conn.execute(
                """CREATE TABLE data_quality_log (
                    log_id         INTEGER PRIMARY KEY,
                    check_code     TEXT NOT NULL,
                    severity       TEXT NOT NULL CHECK (severity IN ('error','warn','info')),
                    layer          TEXT NOT NULL,
                    source_file    TEXT,
                    row_number     INTEGER,
                    company_id     TEXT,
                    period         TEXT,
                    field          TEXT,
                    observed_value TEXT,
                    message        TEXT NOT NULL,
                    action         TEXT NOT NULL
                )"""
            )
            conn.executemany(
                "INSERT INTO data_quality_log (check_code, severity, layer, source_file,"
                " row_number, company_id, period, field, observed_value, message, action)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                self.entries,
            )
