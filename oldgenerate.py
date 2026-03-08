import os
import time
import random
from datetime import datetime, timedelta, date

import psycopg2
import requests
from faker import Faker

fake = Faker()

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DB = os.environ.get("PG_DB", "bankingdb")
PG_USER = os.environ.get("PG_USER", "banking")
PG_PASS = os.environ.get("PG_PASS", "banking_pass")

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

def ch_insert_transactions(rows):
    if not rows:
        return
    lines = []
    for r in rows:
        # transaction_id, account_id, transaction_type, amount, transaction_date, description, destination_account_id
        tx_id, acc_id, tx_type, amt, tx_dt, desc, dst = r
        dst_val = "\\N" if dst is None else str(dst)
        desc_val = (desc or "").replace("\t", " ").replace("\n", " ").replace("\r", " ")
        lines.append(f"{tx_id}\t{acc_id}\t{tx_type}\t{amt}\t{tx_dt}\t{desc_val}\t{dst_val}")
    data = "\n".join(lines) + "\n"
    url = f"{CH_HOST}/?query=INSERT%20INTO%20{CH_DB}.bank_transaction%20FORMAT%20TabSeparated"
    auth = (CH_USER, CH_PASS) if CH_USER else None
    resp = requests.post(url, data=data.encode("utf-8"), auth=auth, timeout=10)
    if resp.status_code >= 400:
         raise RuntimeError(f"ClickHouse insert failed {resp.status_code}: {resp.text[:500]}")


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

            # contacts
            cur.execute(
                "INSERT INTO customer_email(customer_id, email) VALUES (%s,%s);",
                (customer_id, fake.unique.email()),
            )
            cur.execute(
                "INSERT INTO customer_phone(customer_id, phone_type, phone_number) VALUES (%s,%s,%s);",
                (customer_id, "MOBILE", fake.unique.msisdn()[:15]),
            )

            # accounts
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

            # cards
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

            # credit score
            cur.execute(
                "INSERT INTO credit_score(customer_id, score, evaluated_at) VALUES (%s,%s,%s);",
                (customer_id, random.randint(450, 820), datetime.utcnow()),
            )

    conn.commit()

def generate_transactions(conn, n_tx=200, sleep_seconds=2):
    inserted_rows = []

    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM account ORDER BY account_id;")
        accounts = [r[0] for r in cur.fetchall()]
        if len(accounts) < 2:
            conn.commit()
            return

        # Ensure there is money in the system: do some initial deposits each loop
        warmup_deposits = max(5, n_tx // 10)

        def insert_one(src, tx_type, amount, dst=None):
            cur.execute(
                """
                INSERT INTO bank_transaction(account_id, transaction_type, amount, description, destination_account_id)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING transaction_id, account_id, transaction_type, amount, transaction_date, description, destination_account_id;
                """,
                (src, tx_type, amount, f"{tx_type} via generator", dst),
            )
            return cur.fetchone()

        # Warm-up: deposit into random accounts so later withdrawals/transfers can succeed
        for _ in range(warmup_deposits):
            src = random.choice(accounts)
            amount = round(random.uniform(200, 1000), 2)
            try:
                row = insert_one(src, "DEPOSIT", amount)
                inserted_rows.append(row)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Warmup deposit failed: {e}")

        # Main loop: attempt random tx, but handle failures per transaction
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
                row = insert_one(src, tx_type, amount, dst)
                inserted_rows.append(row)
                conn.commit()
            except Exception as e:
                # Insufficient funds or constraint violations: rollback that one insert and continue
                conn.rollback()
                # For demo, keep logs short:
                # print(f"Tx failed ({tx_type}): {e}")
                continue

    if MIRROR_TO_CLICKHOUSE and inserted_rows:
        try:
            ch_insert_transactions(inserted_rows)
        except Exception as e:
            print(f"ClickHouse mirror failed: {e}")

    time.sleep(sleep_seconds)


"""
def generate_transactions(conn, n_tx=200, sleep_seconds=2):
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM account ORDER BY account_id;")
        accounts = [r[0] for r in cur.fetchall()]
        if len(accounts) < 2:
            return

        inserted_rows = []

        for _ in range(n_tx):
            src = random.choice(accounts)
            tx_type = random.choices(
                ["DEPOSIT", "WITHDRAWAL", "TRANSFER"],
                weights=[0.45, 0.35, 0.20],
                k=1
            )[0]
            amount = round(random.uniform(5, 500), 2)

            dst = None
            if tx_type == "TRANSFER":
                dst = src
                while dst == src:
                    dst = random.choice(accounts)

            desc = f"{tx_type} via generator"

            # Insert into Postgres. Trigger adjusts balances.
            cur.execute(
               
                INSERT INTO bank_transaction(account_id, transaction_type, amount, description, destination_account_id)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING transaction_id, account_id, transaction_type, amount, transaction_date, description, destination_account_id;
                ,
                (src, tx_type, amount, desc, dst),
            )
            row = cur.fetchone()
            inserted_rows.append(row)

        conn.commit()

    if MIRROR_TO_CLICKHOUSE:
        ch_insert_transactions(inserted_rows)"""


def main():
    while True:
        try:
            conn = pg_connect()
            seed_reference_data(conn)
            seed_customers_accounts(conn, n_customers=50)
            generate_transactions(conn, n_tx=200, sleep_seconds=2)
            conn.close()
        except Exception as e:
            print(f"Generator error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
