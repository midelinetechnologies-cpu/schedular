"""
One-time migration: Railway PostgreSQL → external MySQL
Run: python migrate.py
"""
import json
import psycopg2
import psycopg2.extras
import pymysql
import pymysql.cursors
from dotenv import load_dotenv
import os

load_dotenv()

# ── Source: Railway PostgreSQL ─────────────────────────────────────────────────
PG_URL = os.environ["DATABASE_URL"]

# ── Destination: Hostinger MySQL (read from env vars) ─────────────────────────
MYSQL_HOST     = os.environ.get("MYSQL_HOST")
MYSQL_USER     = os.environ.get("MYSQL_USER")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE")
MYSQL_PORT     = int(os.environ.get("MYSQL_PORT"))


def pg_conn():
    return psycopg2.connect(
        PG_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=10,
        sslmode="require",
    )


def mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def ensure_mysql_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS extracted_data (
            id         INT AUTO_INCREMENT PRIMARY KEY,
            name       TEXT          DEFAULT '',
            emails     JSON,
            phones     JSON,
            urls       JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS validated_urls (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            url           TEXT          NOT NULL,
            status_code   INT,
            response_time FLOAT,
            content_type  TEXT          DEFAULT '',
            server        TEXT          DEFAULT '',
            redirect_url  TEXT          DEFAULT '',
            checked_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS not_validated_urls (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            url           TEXT          NOT NULL,
            error_message TEXT          DEFAULT '',
            status_code   INT,
            checked_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)


def _migrate_table(pg, my, table, pg_columns, mysql_insert_sql, row_to_values):
    col_list = ", ".join(pg_columns)

    with pg.cursor() as pg_cur:
        pg_cur.execute(f"SELECT COUNT(*) AS total FROM {table}")
        total = pg_cur.fetchone()["total"]
        print(f"[{table}] Found {total} rows in PostgreSQL")

        if total == 0:
            return

        pg_cur.execute(f"SELECT {col_list} FROM {table} ORDER BY id")
        rows = pg_cur.fetchall()

    with my.cursor() as my_cur:
        my_cur.execute(f"SELECT id FROM {table}")
        existing_ids = {r["id"] for r in my_cur.fetchall()}

    new_rows = [r for r in rows if r["id"] not in existing_ids]
    skipped  = len(rows) - len(new_rows)
    inserted = 0
    deleted  = 0

    CHUNK_SIZE = 500
    for offset in range(0, len(new_rows), CHUNK_SIZE):
        chunk = new_rows[offset : offset + CHUNK_SIZE]
        chunk_ids = [row["id"] for row in chunk]

        values = [row_to_values(row) for row in chunk]
        with my.cursor() as my_cur:
            my_cur.executemany(mysql_insert_sql, values)
        my.commit()
        inserted += len(chunk)
        print(f"  [{table}] Migrated {inserted}/{len(new_rows)} rows to MySQL...")

        with pg.cursor() as pg_cur:
            pg_cur.execute(
                f"DELETE FROM {table} WHERE id = ANY(%s)",
                (chunk_ids,),
            )
            pg.commit()
            deleted += pg_cur.rowcount
        print(f"  [{table}] Deleted {deleted} rows from Railway so far...")

    print(f"[{table}] Done — {inserted} inserted, {skipped} skipped, {deleted} deleted from Railway")


def migrate():
    print("Connecting to Railway PostgreSQL...")
    pg = pg_conn()

    print("Connecting to MySQL...")
    my = mysql_conn()

    with my.cursor() as my_cur:
        ensure_mysql_tables(my_cur)
    my.commit()

    # ── extracted_data ────────────────────────────────────────────────────
    _migrate_table(
        pg, my,
        table="extracted_data",
        pg_columns=["id", "name", "emails", "phones", "urls", "created_at"],
        mysql_insert_sql="""
            INSERT INTO extracted_data (id, name, emails, phones, urls, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """,
        row_to_values=lambda r: (
            r["id"],
            r["name"],
            json.dumps(r["emails"] or []),
            json.dumps(r["phones"] or []),
            json.dumps(r["urls"]   or []),
            r["created_at"],
        ),
    )

    # ── validated_urls ────────────────────────────────────────────────────
    _migrate_table(
        pg, my,
        table="validated_urls",
        pg_columns=["id", "url", "status_code", "response_time", "content_type", "server", "redirect_url", "checked_at"],
        mysql_insert_sql="""
            INSERT INTO validated_urls (id, url, status_code, response_time, content_type, server, redirect_url, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        row_to_values=lambda r: (
            r["id"],
            r["url"],
            r["status_code"],
            r["response_time"],
            r["content_type"] or "",
            r["server"] or "",
            r["redirect_url"] or "",
            r["checked_at"],
        ),
    )

    # ── not_validated_urls ────────────────────────────────────────────────
    _migrate_table(
        pg, my,
        table="not_validated_urls",
        pg_columns=["id", "url", "error_message", "status_code", "checked_at"],
        mysql_insert_sql="""
            INSERT INTO not_validated_urls (id, url, error_message, status_code, checked_at)
            VALUES (%s, %s, %s, %s, %s)
        """,
        row_to_values=lambda r: (
            r["id"],
            r["url"],
            r["error_message"] or "",
            r["status_code"],
            r["checked_at"],
        ),
    )

    pg.close()
    my.close()

    print("All tables migrated successfully.")


if __name__ == "__main__":
    migrate()
