use std::env;
use std::error::Error;
use std::fmt::Debug;
use std::str::FromStr;

use serde::Deserialize;
use std::collections::HashMap;
use std::process::exit;

#[derive(Debug, Deserialize)]
struct TxInputRecord {
    #[serde(rename = "type")]
    tx_type: String,
    client: u16,
    tx: u32,
    amount: String,
}

// Expect amount to be currency subunit, fraction of main unit like cents for USD
#[derive(Debug,Clone)]
struct TransactionMessage {
  tx_time: u32,
  tx: u32,
  tx_type: TransactionType,
  client: u16,
  amount: u64,
}

// Limit tx_type to either WITHDRAWAL or DEPOSIT
#[derive(Debug,Clone)]
struct Tx {
    tx: u32,
    tx_type: TransactionType,
    client: u16,
    amount: u64,
    disputed: bool,
}

#[derive(Debug,Clone)]
enum TransactionType {
  WITHDRAWAL,
  DEPOSIT,
  DISPUTE,
  RESOLVE,
  CHARGEBACK,
}

// Expect available, held, total to be currency subunit, fraction of main unit
#[derive(Debug)]
struct Account {
    client: u16,
    available: u64,
    held: u64,
    total: u64,
    locked: bool,
}

impl std::str::FromStr for TransactionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "withdraw" => Ok(TransactionType::WITHDRAWAL),
            "deposit" => Ok(TransactionType::DEPOSIT),
            "dispute" => Ok(TransactionType::DISPUTE),
            "resolve" => Ok(TransactionType::RESOLVE),
            "chargeback" => Ok(TransactionType::CHARGEBACK),
            _ => Err(format!("'{}' is not a valid TransactionType", s)),
        }
    }
}

#[derive(Debug)]
struct AccountTransactions {
    txs_txid: HashMap<u32, Tx>,
    account_client: HashMap<u16, Account>,
    tx_msgs_time: HashMap<u32, TransactionMessage>
}

impl AccountTransactions {
    fn new() -> AccountTransactions {
        AccountTransactions {
            txs_txid: HashMap::new(),
            tx_msgs_time: HashMap::new(),
            account_client: HashMap::new(),
        }
    }

    fn handle_tx_message(&mut self, transaction_msg: &TransactionMessage) {
        self.tx_msgs_time.insert(transaction_msg.tx_time, (*transaction_msg).clone());

        match transaction_msg.tx_type  {
            TransactionType::DEPOSIT => self.deposit_tx(transaction_msg),
            TransactionType::WITHDRAWAL => self.withdrawal_tx(transaction_msg),
            TransactionType::DISPUTE => self.dispute_tx(transaction_msg),
            TransactionType::RESOLVE => self.resolve_tx(transaction_msg),
            TransactionType::CHARGEBACK => self.chargeback_tx(transaction_msg)
        }
    }

    fn deposit_tx(&mut self, transaction_msg: &TransactionMessage) {
        self.txs_txid.insert(transaction_msg.tx,
                             Tx {
                                 tx: transaction_msg.tx,
                                 tx_type: transaction_msg.tx_type.clone(),
                                 client: transaction_msg.client,
                                 amount: transaction_msg.amount,
                                 disputed: false,
                             });
        if let Some(acct) = self.account_client.get_mut(&transaction_msg.client) {
            acct.available += transaction_msg.amount;
            acct.total = acct.available + acct.held;
        } else {
            let new_acct = Account {
                client: transaction_msg.client,
                available: transaction_msg.amount,
                held: 0,
                total: transaction_msg.amount,
                locked: false,
            };
            self.account_client.insert(transaction_msg.client, new_acct);
        }
    }

    fn withdrawal_tx(&mut self, transaction_msg: &TransactionMessage) {
        self.txs_txid.insert(transaction_msg.tx,
                             Tx {
                                 tx: transaction_msg.tx,
                                 tx_type: transaction_msg.tx_type.clone(),
                                 client: transaction_msg.client,
                                 amount: transaction_msg.amount,
                                 disputed: false,
                             });
        if let Some(acct) = self.account_client.get_mut(&transaction_msg.client) {
            if acct.available >= transaction_msg.amount {
                acct.available -= transaction_msg.amount;
                acct.total = acct.available + acct.held;
            } else {
                eprintln!("Insufficient funds for withdrawal. Ignored transaction. Client: {}, Transaction ID: {}.",
                          transaction_msg.client, transaction_msg.tx);
            }

        } else {
            let new_acct = Account {
                client: transaction_msg.client,
                available: 0,
                held: 0,
                total: 0,
                locked: false,
            };
            self.account_client.insert(transaction_msg.client, new_acct);
            eprintln!("Ignored withdrawal on non-existent client, {}. New client account created with 0.000 total balance.", transaction_msg.client);
        }
    }

    fn dispute_tx(&mut self, transaction_msg: &TransactionMessage) {
        if let Some(acct) = self.account_client.get_mut(&transaction_msg.client) {
            if let Some(tx) = self.txs_txid.get_mut(&transaction_msg.tx) {
                if tx.amount >= acct.available {
                    acct.held += tx.amount;
                    acct.available -= tx.amount;
                    tx.disputed = true;
                } else {
                    eprintln!("Unable to hold funds for dispute of transaction, {}, from client, {}. Ignoring dispute.", transaction_msg.tx, transaction_msg.client);
                }
            } else {
                eprintln!("Failed to location transaction, {}. Ignoring dispute.", transaction_msg.tx);
            }

        } else {
            let new_acct = Account {
                client: transaction_msg.client,
                available: 0,
                held: 0,
                total: 0,
                locked: false,
            };
            self.account_client.insert(transaction_msg.client, new_acct);
            eprintln!("Ignored dispute on non-existent client, {}. New client account created with 0.000 total balance.", transaction_msg.client);
        }
    }

    fn resolve_tx(&mut self, transaction_msg: &TransactionMessage) {
        if let Some(acct) = self.account_client.get_mut(&transaction_msg.client) {
            if let Some(tx) = self.txs_txid.get_mut(&transaction_msg.tx) {
                if tx.disputed && tx.amount <= acct.held {
                    acct.held -= tx.amount;
                    acct.available += tx.amount;
                    tx.disputed = false;
                } else {
                    eprintln!("Unable to resolve held funds for disputed transaction, {}, from client, {}. Ignoring resolve.", transaction_msg.tx, transaction_msg.client);
                }
            } else {
                eprintln!("Failed to location transaction, {}. Ignoring resolve.", transaction_msg.tx);
            }

        } else {
            let new_acct = Account {
                client: transaction_msg.client,
                available: 0,
                held: 0,
                total: 0,
                locked: false,
            };
            self.account_client.insert(transaction_msg.client, new_acct);
            eprintln!("Ignored resolve on non-existent client, {}. New client account created with 0.000 total balance.", transaction_msg.client);
        }
    }

    fn chargeback_tx(&mut self, transaction_msg: &TransactionMessage) {
        if let Some(acct) = self.account_client.get_mut(&transaction_msg.client) {
            if let Some(tx) = self.txs_txid.get_mut(&transaction_msg.tx) {
                if tx.disputed && tx.amount <= acct.held {
                    acct.held -= tx.amount;
                    acct.locked = true;
                    tx.disputed = false;
                } else {
                    eprintln!("Failed to complete chargeback. Hold less chargeback amount: {}, Disputed: {}, transaction: {}.",
                              acct.held - tx.amount, tx.disputed, transaction_msg.tx);
                }
            } else {
                eprintln!("Failed to location transaction, {}. Ignoring resolve.", transaction_msg.tx);
            }

        } else {
            let new_acct = Account {
                client: transaction_msg.client,
                available: 0,
                held: 0,
                total: 0,
                locked: false,
            };
            self.account_client.insert(transaction_msg.client, new_acct);
            eprintln!("Ignored chargeback_tx on non-existent client, {}. New client account created with 0.000 total balance.", transaction_msg.client);
        }

    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];

    let mut account_txs = AccountTransactions::new();
    let result = read_file(filename, &mut account_txs);
    match result {
        Ok(_) => { eprintln!("Read the input file, {}.", filename); }
        Err(_) => { eprintln!("Input file read failed, {}", filename); exit(1) }
    }

    output_accounts(&account_txs);
}

fn read_file(filename: &String, account_txs: &mut AccountTransactions) -> Result<(), Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(filename)?;
    let mut counter: u32 = 1;
    for result in rdr.deserialize() {
        let record: TxInputRecord = result?;
        let message = input_record_to_transaction(&record, counter);
        account_txs.handle_tx_message(&message);
        counter += 1;
    }
    Ok(())
}

fn output_accounts(accts: &AccountTransactions) {
    println!("client,available,held,total,locked");
    for (client, account) in accts.account_client.iter() {
        println!("{},{},{},{},{}",
                 client, to_currency_unit(account.available),
                 to_currency_unit(account.held),
                 to_currency_unit(account.total),
                 account.locked);
    }
}

fn to_subunit(amount_unit: &String) -> u64 {
    let amount_orig: f64 = amount_unit.parse().expect("Failed to convert to floating point.");
    (amount_orig * 1.0e+4_f64) as u64
}

fn to_currency_unit(amount_subunit: u64) -> f64 {
    amount_subunit as f64 / 1.0e+4_f64
}

fn input_record_to_transaction(record: &TxInputRecord, time: u32) -> TransactionMessage {
    let converted_amount = if record.amount.len() > 0 {
        to_subunit(&(record.amount))
    } else {
        0 as u64
    };

    TransactionMessage {
        tx_time: time,
        tx: record.tx,
        tx_type: TransactionType::from_str(&record.tx_type.as_str()).expect("Failed to convert tx_type"),
        client: record.client,
        amount: converted_amount,
    }
}