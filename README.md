# tx_account

A Rust example of processing payment-related transactions.

## Build

```shell script
cargo build
```

## Run

```shell script
cargo run -- inputdata.csv > accounts.csv 
```

Error messages sent to STDERR

## Discussion

The application reads 5 different types of transactions from the input file. The transaction types are:

```rust
enum TransactionType {
  WITHDRAWAL,
  DEPOSIT,
  DISPUTE,
  RESOLVE,
  CHARGEBACK,
}
```

Each transaction impacts a client account's available, held, and total balances. DEPOSIT's credit or add to the
available balance. WITHDRAWAL's debit or subtract from the available balance.