from urllib.parse import quote_plus
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import column
from pathlib import Path

class InputDatabase:

    def __init__(self, aidb_path, yield_path, yield_interval):
        self.aidb_path = Path(aidb_path).resolve()
        self.yield_path = Path(yield_path).resolve()
        self.yield_interval = yield_interval

    def create(self, classifiers, output_path):
        raise NotImplementedError()

    def get_disturbance_types(self):
        with self._connect() as conn:
            dist_type_table = table("tbldisturbancetypedefault", column("disttypename"))
            dist_types = {
                row[0] for row in conn.execute(
                    dist_type_table
                        .select(dist_type_table.c.disttypename)
                        .distinct()
                )
            }
            
            return dist_types

    @contextmanager
    def _connect(self):
        connection_string = quote_plus(
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={self.aidb_path};"
            r"ExtendedAnsiSQL=1;"
        )

        engine = create_engine(f"access+pyodbc:///?odbc_connect={connection_string}")

        try:
            with engine.connect() as conn:
                yield conn
        finally:
            engine.dispose()
