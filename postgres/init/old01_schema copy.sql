-- schema.sql
-- PostgreSQL (psql) schema

BEGIN;

-- Clean drop
DROP TABLE IF EXISTS bank_transaction CASCADE;
DROP TABLE IF EXISTS credit_score CASCADE;
DROP TABLE IF EXISTS loan CASCADE;
DROP TABLE IF EXISTS card CASCADE;
DROP TABLE IF EXISTS account CASCADE;
DROP TABLE IF EXISTS employee CASCADE;
DROP TABLE IF EXISTS customer_email CASCADE;
DROP TABLE IF EXISTS customer_phone CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS card_type CASCADE;
DROP TABLE IF EXISTS loan_type CASCADE;
DROP TABLE IF EXISTS account_type CASCADE;
DROP TABLE IF EXISTS branch CASCADE;

DROP FUNCTION IF EXISTS fn_apply_bank_transaction();

-- =========================
-- Core reference tables
-- =========================

CREATE TABLE branch (
  branch_id   serial PRIMARY KEY,
  branch_name varchar(100) NOT NULL,
  branch_city varchar(100) NOT NULL
);

CREATE TABLE account_type (
  account_type_code varchar(20) PRIMARY KEY,
  description       varchar(100) NOT NULL
);

CREATE TABLE loan_type (
  loan_type_code varchar(20) PRIMARY KEY,
  description    varchar(100) NOT NULL
);

CREATE TABLE card_type (
  card_type_code varchar(20) PRIMARY KEY,
  description    varchar(100) NOT NULL
);

-- =========================
-- Customer domain
-- =========================

CREATE TABLE customer (
  customer_id  serial PRIMARY KEY,
  branch_id    int NOT NULL,
  first_name   varchar(50) NOT NULL,
  last_name    varchar(50) NOT NULL,
  national_id  varchar(20) NOT NULL UNIQUE,
  address      text,
  created_at   timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_customer_branch
    FOREIGN KEY (branch_id) REFERENCES branch(branch_id)
);

CREATE TABLE customer_phone (
  customer_id  int NOT NULL,
  phone_type   varchar(20),
  phone_number varchar(20) NOT NULL,
  PRIMARY KEY (customer_id, phone_number),
  CONSTRAINT fk_customer_phone_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
    ON DELETE CASCADE
);

CREATE TABLE customer_email (
  customer_id int NOT NULL,
  email       varchar(100) NOT NULL,
  PRIMARY KEY (customer_id, email),
  CONSTRAINT fk_customer_email_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
    ON DELETE CASCADE
);

-- =========================
-- Staff domain
-- Unify Employee and Manager into a single table
-- Enforce at most one MANAGER per branch with a partial unique index
-- =========================

CREATE TABLE employee (
  employee_id serial PRIMARY KEY,
  first_name  varchar(50) NOT NULL,
  last_name   varchar(50) NOT NULL,
  branch_id   int NOT NULL,
  position    varchar(50) NOT NULL,
  hire_date   date NOT NULL,
  salary      numeric(15,2) NOT NULL,
  national_id varchar(20),
  CONSTRAINT fk_employee_branch
    FOREIGN KEY (branch_id) REFERENCES branch(branch_id),
  CONSTRAINT uq_employee_national_id UNIQUE (national_id),
  CONSTRAINT ck_employee_salary_positive CHECK (salary > 0)
);

-- One manager per branch (only rows whose position is 'MANAGER')
CREATE UNIQUE INDEX uq_one_manager_per_branch
ON employee(branch_id)
WHERE position = 'MANAGER';

-- =========================
-- Accounts, cards, loans
-- Note: account.branch_id removed to avoid inconsistency with customer.branch_id
-- Branch can be derived via customer
-- =========================

CREATE TABLE account (
  account_id        serial PRIMARY KEY,
  customer_id       int NOT NULL,
  account_number    varchar(26) NOT NULL UNIQUE,
  account_type_code varchar(20) NOT NULL,
  opened_at         date NOT NULL,
  status            varchar(20) NOT NULL,
  balance           numeric(15,2) NOT NULL DEFAULT 0,
  CONSTRAINT fk_account_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  CONSTRAINT fk_account_account_type
    FOREIGN KEY (account_type_code) REFERENCES account_type(account_type_code),
  CONSTRAINT ck_account_balance_nonnegative CHECK (balance >= 0),
  CONSTRAINT ck_account_status CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED'))
);

CREATE TABLE card (
  card_id          serial PRIMARY KEY,
  card_number      varchar(16) NOT NULL UNIQUE,
  card_type_code   varchar(20) NOT NULL,
  expiration_date  date NOT NULL,
  cvv              varchar(4) NOT NULL,
  issued_at        date NOT NULL,
  status           varchar(20) NOT NULL,
  customer_id      int NOT NULL,
  CONSTRAINT fk_card_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  CONSTRAINT fk_card_card_type
    FOREIGN KEY (card_type_code) REFERENCES card_type(card_type_code),
  CONSTRAINT ck_card_status CHECK (status IN ('ACTIVE', 'BLOCKED', 'EXPIRED', 'CLOSED')),
  CONSTRAINT ck_card_cvv_digits CHECK (cvv ~ '^[0-9]{3,4}$')
);

CREATE TABLE loan (
  loan_id        serial PRIMARY KEY,
  customer_id    int NOT NULL,
  account_id     int,
  loan_type_code varchar(20) NOT NULL,
  amount         numeric(15,2) NOT NULL,
  interest_rate  numeric(5,2) NOT NULL,
  start_date     date NOT NULL,
  end_date       date NOT NULL,
  status         varchar(20) NOT NULL,
  CONSTRAINT fk_loan_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  CONSTRAINT fk_loan_account
    FOREIGN KEY (account_id) REFERENCES account(account_id),
  CONSTRAINT fk_loan_loan_type
    FOREIGN KEY (loan_type_code) REFERENCES loan_type(loan_type_code),
  CONSTRAINT ck_loan_amount_positive CHECK (amount > 0),
  CONSTRAINT ck_loan_interest_nonnegative CHECK (interest_rate >= 0),
  CONSTRAINT ck_loan_dates CHECK (end_date >= start_date),
  CONSTRAINT ck_loan_status CHECK (status IN ('PENDING', 'ACTIVE', 'PAID', 'DEFAULTED', 'CLOSED'))
);

CREATE TABLE credit_score (
  credit_score_id serial PRIMARY KEY,
  customer_id     int NOT NULL,
  score           int NOT NULL,
  evaluated_at    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_credit_score_customer
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
  CONSTRAINT ck_credit_score_range CHECK (score BETWEEN 300 AND 850)
);

-- =========================
-- Transactions
-- Stronger constraints + default timestamp
-- destination_account_id must be present only for TRANSFER
-- =========================

CREATE TABLE bank_transaction (
  transaction_id         serial PRIMARY KEY,
  account_id             int NOT NULL,
  transaction_type       varchar(20) NOT NULL,
  amount                 numeric(15,2) NOT NULL,
  transaction_date       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  description            varchar(255),
  destination_account_id int,
  CONSTRAINT fk_tx_account
    FOREIGN KEY (account_id) REFERENCES account(account_id),
  CONSTRAINT fk_tx_destination_account
    FOREIGN KEY (destination_account_id) REFERENCES account(account_id),
  CONSTRAINT ck_tx_amount_positive CHECK (amount > 0),
  CONSTRAINT ck_tx_type CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')),
  CONSTRAINT ck_tx_destination_rules CHECK (
    (transaction_type = 'TRANSFER' AND destination_account_id IS NOT NULL AND destination_account_id <> account_id)
    OR
    (transaction_type IN ('DEPOSIT', 'WITHDRAWAL') AND destination_account_id IS NULL)
  )
);

-- =========================
-- Trigger: maintain balances
-- Uses exceptions instead of silently ignoring invalid inserts
-- Uses consistent locking order for transfers to avoid deadlocks
-- =========================

CREATE OR REPLACE FUNCTION fn_apply_bank_transaction()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  v_src_balance numeric(15,2);
  v_dst_balance numeric(15,2);
  v_first_id    int;
  v_second_id   int;
BEGIN
  -- Amount check is also enforced by table constraint, but keep a clear message here
  IF NEW.amount IS NULL OR NEW.amount <= 0 THEN
    RAISE EXCEPTION 'Transaction amount must be > 0';
  END IF;

  IF NEW.transaction_type = 'DEPOSIT' THEN
    -- Lock source
    SELECT balance INTO v_src_balance
    FROM account
    WHERE account_id = NEW.account_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Source account % does not exist', NEW.account_id;
    END IF;

    UPDATE account
    SET balance = balance + NEW.amount
    WHERE account_id = NEW.account_id;

    RETURN NEW;

  ELSIF NEW.transaction_type = 'WITHDRAWAL' THEN
    -- Lock source
    SELECT balance INTO v_src_balance
    FROM account
    WHERE account_id = NEW.account_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Source account % does not exist', NEW.account_id;
    END IF;

    IF v_src_balance < NEW.amount THEN
      RAISE EXCEPTION 'Insufficient funds in account % (balance %, requested %)',
        NEW.account_id, v_src_balance, NEW.amount;
    END IF;

    UPDATE account
    SET balance = balance - NEW.amount
    WHERE account_id = NEW.account_id;

    RETURN NEW;

  ELSIF NEW.transaction_type = 'TRANSFER' THEN
    -- destination rules are enforced by CHECK, but keep clear messaging here too
    IF NEW.destination_account_id IS NULL THEN
      RAISE EXCEPTION 'TRANSFER requires destination_account_id';
    END IF;
    IF NEW.destination_account_id = NEW.account_id THEN
      RAISE EXCEPTION 'TRANSFER destination_account_id must differ from account_id';
    END IF;

    -- Deadlock-safe locking: always lock lower id first
    v_first_id  := LEAST(NEW.account_id, NEW.destination_account_id);
    v_second_id := GREATEST(NEW.account_id, NEW.destination_account_id);

    -- Lock both accounts in deterministic order
    PERFORM 1
    FROM account
    WHERE account_id IN (v_first_id, v_second_id)
    ORDER BY account_id
    FOR UPDATE;

    -- Verify source exists and get balances
    SELECT balance INTO v_src_balance
    FROM account
    WHERE account_id = NEW.account_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Source account % does not exist', NEW.account_id;
    END IF;

    SELECT balance INTO v_dst_balance
    FROM account
    WHERE account_id = NEW.destination_account_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'Destination account % does not exist', NEW.destination_account_id;
    END IF;

    IF v_src_balance < NEW.amount THEN
      RAISE EXCEPTION 'Insufficient funds in account % (balance %, requested %)',
        NEW.account_id, v_src_balance, NEW.amount;
    END IF;

    UPDATE account
    SET balance = balance - NEW.amount
    WHERE account_id = NEW.account_id;

    UPDATE account
    SET balance = balance + NEW.amount
    WHERE account_id = NEW.destination_account_id;

    RETURN NEW;

  ELSE
    RAISE EXCEPTION 'Invalid transaction_type: %', NEW.transaction_type;
  END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_apply_bank_transaction ON bank_transaction;

CREATE TRIGGER trg_apply_bank_transaction
BEFORE INSERT ON bank_transaction
FOR EACH ROW
EXECUTE FUNCTION fn_apply_bank_transaction();

-- =========================
-- Performance indexes for common joins and Grafana dashboards
-- =========================

CREATE INDEX idx_customer_branch_id ON customer(branch_id);

CREATE INDEX idx_employee_branch_id ON employee(branch_id);

CREATE INDEX idx_account_customer_id ON account(customer_id);
CREATE INDEX idx_account_type_code   ON account(account_type_code);

CREATE INDEX idx_card_customer_id    ON card(customer_id);
CREATE INDEX idx_card_type_code      ON card(card_type_code);

CREATE INDEX idx_loan_customer_id    ON loan(customer_id);
CREATE INDEX idx_loan_account_id     ON loan(account_id);
CREATE INDEX idx_loan_type_code      ON loan(loan_type_code);

CREATE INDEX idx_credit_score_customer_id ON credit_score(customer_id);

CREATE INDEX idx_tx_account_id       ON bank_transaction(account_id);
CREATE INDEX idx_tx_destination_id   ON bank_transaction(destination_account_id);
CREATE INDEX idx_tx_date             ON bank_transaction(transaction_date);

COMMIT;
