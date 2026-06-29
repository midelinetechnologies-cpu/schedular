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


def ensure_mysql_table(cur):
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


def migrate():
    print("Connecting to Railway PostgreSQL...")
    pg = pg_conn()

    print("Connecting to MySQL...")
    my = mysql_conn()

    with pg.cursor() as pg_cur:
        pg_cur.execute("SELECT COUNT(*) AS total FROM extracted_data")
        total = pg_cur.fetchone()["total"]
        print(f"Found {total} rows in PostgreSQL")

        pg_cur.execute(
            "SELECT id, name, emails, phones, urls, created_at FROM extracted_data ORDER BY id"
        )
        rows = pg_cur.fetchall()

    CHUNK_SIZE = 500

    with my.cursor() as my_cur:
        ensure_mysql_table(my_cur)
        my_cur.execute("SELECT id FROM extracted_data")
        existing_ids = {r["id"] for r in my_cur.fetchall()}

    new_rows = [r for r in rows if r["id"] not in existing_ids]
    skipped  = len(rows) - len(new_rows)
    inserted = 0
    deleted  = 0

    for offset in range(0, len(new_rows), CHUNK_SIZE):
        chunk = new_rows[offset : offset + CHUNK_SIZE]
        chunk_ids = [row["id"] for row in chunk]

        # ── Step 1: insert chunk into MySQL ───────────────────────────────
        values = [
            (
                row["id"],
                row["name"],
                json.dumps(row["emails"] or []),
                json.dumps(row["phones"] or []),
                json.dumps(row["urls"]   or []),
                row["created_at"],
            )
            for row in chunk
        ]
        with my.cursor() as my_cur:
            my_cur.executemany(
                """
                INSERT INTO extracted_data (id, name, emails, phones, urls, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                values,
            )
        my.commit()
        inserted += len(chunk)
        print(f"  Migrated {inserted}/{len(new_rows)} rows to MySQL...")

        # ── Step 2: delete that same chunk from Railway PostgreSQL ────────
        with pg.cursor() as pg_cur:
            pg_cur.execute(
                "DELETE FROM extracted_data WHERE id = ANY(%s)",
                (chunk_ids,),
            )
            pg.commit()
            deleted += pg_cur.rowcount
        print(f"  Deleted {deleted} rows from Railway so far...")

    pg.close()
    my.close()

    print(f"Done — {inserted} inserted, {skipped} skipped, {deleted} deleted from Railway")


if __name__ == "__main__":
    migrate()
