CREATE DATABASE IF NOT EXISTS banking;

CREATE TABLE IF NOT EXISTS banking.bank_transaction
(
  transaction_id UInt64,
  account_id UInt64,
  transaction_type LowCardinality(String),
  amount Decimal(15, 2),
  transaction_date DateTime,
  description String,
  destination_account_id Nullable(UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_date, account_id);

-- Optional: a materialized view style rollup can be added later
