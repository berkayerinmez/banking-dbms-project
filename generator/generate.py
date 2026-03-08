import os
import time
import random
from datetime import datetime, timedelta, date
from decimal import Decimal

import psycopg2
import requests
from faker import Faker

fake = Faker()

# Postgres
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = os.environ.get("PG_DB", "bankingdb")
PG_USER = os.environ.get("PG_USER", "banking")
PG_PASS = os.environ.get("PG_PASS", "banking_pass")

# ClickHouse
CH_HOST = os.environ.get("CH_HOST", "http://clickhouse:8123")
CH_DB = os.environ.get("CH_DB", "banking")
CH_USER = os.environ.get("CH_USER", "")
CH_PASS = os.environ.get("CH_PASS", "")
MIRROR_TO_CLICKHOUSE = os.environ.get("MIRROR_TO_CLICKHOUSE", "true").lower() == "true"


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def ch_http_auth():
    return (CH_USER, CH_PASS) if CH_USER else None


def ch_insert_transactions(rows):
    """
    Inserts into ClickHouse using HTTP + TabSeparated.

    ClickHouse table schema assumed:
      transaction_id UInt64,
      account_id UInt64,
      transaction_type String/LowCardinality(String),
      amount Decimal(15,2),
      transaction_date DateTime,           <-- IMPORTANT: seconds precision
      description String,
      destination_account_id Nullable(UInt64)
    """
    if not rows:
        return

    lines = []
    for r in rows:
        tx_id, acc_id, tx_type, amt, tx_dt, desc, dst = r

        # Ensure integers are plain ints
        tx_id_val = int(tx_id)
        acc_id_val = int(acc_id)

        # Ensure Decimal(15,2) is sent as plain numeric string
        if isinstance(amt, Decimal):
            amt_val = f"{amt:.2f}"
        else:
            amt_val = f"{float(amt):.2f}"

        # CRITICAL: ClickHouse DateTime expects no microseconds unless DateTime64
        if isinstance(tx_dt, datetime):
            tx_dt_val = tx_dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        else:
            # Fallback if it's already a string
            # Try to drop fractional seconds if present
            tx_dt_str = str(tx_dt)
            tx_dt_val = tx_dt_str.split(".")[0]

        desc_val = (desc or "").replace("\t", " ").replace("\n", " ").replace("\r", " ")
        dst_val = "\\N" if dst is None else str(int(dst))

        # 7 columns exactly, tab separated
        lines.append(
            f"{tx_id_val}\t{acc_id_val}\t{tx_type}\t{amt_val}\t{tx_dt_val}\t{desc_val}\t{dst_val}"
        )

    data = "\n".join(lines) + "\n"
    url = f"{CH_HOST}/?query=INSERT%20INTO%20{CH_DB}.bank_transaction%20FORMAT%20TabSeparated"
    resp = requests.post(url, data=data.encode("utf-8"), auth=ch_http_auth(), timeout=15)

    if resp.status_code >= 400:
        # Show server error text (first 800 chars is enough)
        raise RuntimeError(f"ClickHouse insert failed {resp.status_code}: {resp.text[:800]}")


def seed_reference_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM branch;")
        if cur.fetchone()[0] == 0:
            for i in range(3):
                cur.execute(
                    "INSERT INTO branch(branch_name, branch_city) VALUES (%s,%s);",
                    (f"Branch {i+1}", fake.city()),
                )

        cur.execute("SELECT COUNT(*) FROM account_type;")
        if cur.fetchone()[0] == 0:
            cur.executemany(
                "INSERT INTO account_type(account_type_code, description) VALUES (%s,%s);",
                [("CHECKING", "Checking account"), ("SAVINGS", "Savings account")],
            )

        cur.execute("SELECT COUNT(*) FROM loan_type;")
        if cur.fetchone()[0] == 0:
            cur.executemany(
                "INSERT INTO loan_type(loan_type_code, description) VALUES (%s,%s);",
                [("PERSONAL", "Personal loan"), ("AUTO", "Auto loan")],
            )

        cur.execute("SELECT COUNT(*) FROM card_type;")
        if cur.fetchone()[0] == 0:
            cur.executemany(
                "INSERT INTO card_type(card_type_code, description) VALUES (%s,%s);",
                [("DEBIT", "Debit card"), ("CREDIT", "Credit card")],
            )

    conn.commit()


def seed_customers_accounts(conn, n_customers=50):
    with conn.cursor() as cur:
        cur.execute("SELECT branch_id FROM branch ORDER BY branch_id;")
        branches = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT COUNT(*) FROM customer;")
        existing = cur.fetchone()[0]
        if existing >= n_customers:
            return

        for _ in range(n_customers - existing):
            branch_id = random.choice(branches)
            nat_id = str(fake.unique.random_number(digits=10, fix_len=True))

            cur.execute(
                """
                INSERT INTO customer(branch_id, first_name, last_name, national_id, address)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING customer_id;
                """,
                (branch_id, fake.first_name(), fake.last_name(), nat_id, fake.address()),
            )
            customer_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO customer_email(customer_id, email) VALUES (%s,%s);",
                (customer_id, fake.unique.email()),
            )
            cur.execute(
                "INSERT INTO customer_phone(customer_id, phone_type, phone_number) VALUES (%s,%s,%s);",
                (customer_id, "MOBILE", fake.unique.msisdn()[:15]),
            )

            # 1 to 2 accounts per customer
            for _a in range(random.randint(1, 2)):
                acct_num = fake.unique.iban()[:26]
                acct_type = random.choice(["CHECKING", "SAVINGS"])
                opened = date.today() - timedelta(days=random.randint(30, 900))
                status = "ACTIVE"
                cur.execute(
                    """
                    INSERT INTO account(customer_id, account_number, account_type_code, opened_at, status, balance)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    RETURNING account_id;
                    """,
                    (customer_id, acct_num, acct_type, opened, status, 0),
                )

            # 0 to 2 cards
            for _c in range(random.randint(0, 2)):
                card_num = "".join(str(random.randint(0, 9)) for _ in range(16))
                card_type = random.choice(["DEBIT", "CREDIT"])
                exp = date.today() + timedelta(days=365 * random.randint(1, 5))
                cvv = str(random.randint(100, 999))
                issued = date.today() - timedelta(days=random.randint(1, 900))
                cstatus = "ACTIVE"
                cur.execute(
                    """
                    INSERT INTO card(card_number, card_type_code, expiration_date, cvv, issued_at, status, customer_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s);
                    """,
                    (card_num, card_type, exp, cvv, issued, cstatus, customer_id),
                )

            cur.execute(
                "INSERT INTO credit_score(customer_id, score, evaluated_at) VALUES (%s,%s,%s);",
                (customer_id, random.randint(450, 820), datetime.utcnow()),
            )

    conn.commit()


def generate_transactions(conn, n_tx=200, sleep_seconds=2):
    """
    Inserts into Postgres (trigger updates balances). Then mirrors successful rows to ClickHouse.
    Handles insufficient funds and other errors per-transaction so one bad tx does not rollback all.
    """
    inserted_rows = []

    def insert_one(cur, src, tx_type, amount, dst=None):
        cur.execute(
            """
            INSERT INTO bank_transaction(account_id, transaction_type, amount, description, destination_account_id)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING transaction_id, account_id, transaction_type, amount, transaction_date, description, destination_account_id;
            """,
            (src, tx_type, amount, f"{tx_type} via generator", dst),
        )
        return cur.fetchone()

    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM account ORDER BY account_id;")
        accounts = [r[0] for r in cur.fetchall()]
        if len(accounts) < 2:
            conn.commit()
            return

        # Warmup deposits so withdrawals/transfers succeed
        warmup = max(10, n_tx // 10)
        for _ in range(warmup):
            src = random.choice(accounts)
            amount = round(random.uniform(200, 1000), 2)
            try:
                row = insert_one(cur, src, "DEPOSIT", amount, None)
                inserted_rows.append(row)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Warmup deposit failed: {e}")

        # Main tx generation
        for _ in range(n_tx):
            src = random.choice(accounts)
            tx_type = random.choices(
                ["DEPOSIT", "WITHDRAWAL", "TRANSFER"],
                weights=[0.55, 0.25, 0.20],
                k=1
            )[0]
            amount = round(random.uniform(5, 500), 2)
            dst = None

            if tx_type == "TRANSFER":
                dst = src
                while dst == src:
                    dst = random.choice(accounts)

            try:
                row = insert_one(cur, src, tx_type, amount, dst)
                inserted_rows.append(row)
                conn.commit()
            except Exception:
                conn.rollback()
                continue

    # Mirror to ClickHouse in one batch
    if MIRROR_TO_CLICKHOUSE and inserted_rows:
        try:
            ch_insert_transactions(inserted_rows)
        except Exception as e:
            print(f"ClickHouse mirror failed: {e}")

    time.sleep(sleep_seconds)


def main():
    while True:
        conn = None
        try:
            conn = pg_connect()
            seed_reference_data(conn)
            seed_customers_accounts(conn, n_customers=50)
            generate_transactions(conn, n_tx=50, sleep_seconds=5)
        except Exception as e:
            print(f"Generator error: {e}")
            time.sleep(5)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


if __name__ == "__main__":
    main()
