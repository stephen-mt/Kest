from pathlib import Path

import psycopg

from workload.core.config import Settings

SQL_DIR = Path(__file__).with_name("sql")


def main():
    settings = Settings.from_env()
    with (
        psycopg.connect(**settings.pg_kwargs(), autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute((SQL_DIR / "schema.sql").read_text())
        cursor.execute((SQL_DIR / "seed.sql").read_text())
        cursor.execute((SQL_DIR / "constraints.sql").read_text())
        cursor.execute("""
            SELECT relname
            FROM pg_class
            WHERE relnamespace = 'public'::regnamespace AND relkind = 'r'
            ORDER BY relname
        """)
        tables = [row[0] for row in cursor.fetchall()]
    print(f"Workload schema ready: {', '.join(tables)}")


if __name__ == "__main__":
    main()
