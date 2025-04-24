use crate::index::Index;
use crate::segment::{Segment, WalOperation, WalSegment};
use std::io::{Error, ErrorKind};
use std::path::Path;

pub struct Storage {
    segment: Segment,
    index: Index,
    wal: WalSegment,
    in_transaction: bool, // Flag to track if a transaction is in progress
}

impl Storage {
    // Constructor to create a new Storage instance
    pub fn new(path: &Path) -> Result<Self, Error> {
        let segment = Segment::new(path)?; // Create a new Segment from the given path
        let index = Index::new(); // Create a new Index instance
        let wal_path = path.with_extension("wal"); // Set the WAL file path
        let wal = WalSegment::new(&wal_path)?; // Create a new WAL segment
        let mut storage = Self {
            segment,
            index,
            wal,
            in_transaction: false, // Initially, there is no transaction
        };

        // Read operations from the WAL (Write-Ahead Log) and apply them to the storage
        let operations = storage.wal.read_operations()?;
        if !operations.is_empty() {
            for op in operations {
                match op {
                    WalOperation::Set { key, value } => {
                        let offset = storage.segment.set(&key, &value)?; // Perform the SET operation
                        storage.index.insert(&key, offset); // Update the index
                    }
                    WalOperation::Delete { key } => {
                        if let Some(offset) = storage.index.get_offset(&key) {
                            storage.segment.delete(offset)?; // Perform the DELETE operation
                            storage.index.map.remove(&key); // Remove the key from the index
                        }
                    }
                }
            }
            storage.wal.clear()?; // Clear the WAL after processing operations
        }

        Ok(storage)
    }

    // Start a new transaction
    pub fn begin_transaction(&mut self) -> Result<(), Error> {
        if self.in_transaction {
            return Err(Error::new(
                ErrorKind::Other,
                "Transaction already in progress", // Prevent starting a new transaction if one is already active
            ));
        }
        self.in_transaction = true; // Mark the transaction as active
        self.wal.clear()?; // Clear the WAL to start fresh
        Ok(())
    }

    // Commit the current transaction
    pub fn commit(&mut self) -> Result<(), Error> {
        if !self.in_transaction {
            return Err(Error::new(ErrorKind::Other, "No active transaction")); // Ensure a transaction is active
        }
        let operations = self.wal.read_operations()?; // Read the operations from the WAL
        for op in operations {
            match op {
                WalOperation::Set { key, value } => {
                    let offset = self.segment.set(&key, &value)?; // Apply SET operation
                    self.index.insert(&key, offset); // Update the index
                }
                WalOperation::Delete { key } => {
                    if let Some(offset) = self.index.get_offset(&key) {
                        self.segment.delete(offset)?; // Apply DELETE operation
                        self.index.map.remove(&key); // Remove the key from the index
                    }
                }
            }
        }
        self.wal.clear()?; // Clear the WAL after committing operations
        self.in_transaction = false; // Mark the transaction as completed
        Ok(())
    }

    // Rollback the current transaction, clearing any uncommitted changes
    pub fn rollback(&mut self) -> Result<(), Error> {
        self.wal.clear()?; // Clear the WAL to discard the operations
        self.in_transaction = false; // Mark the transaction as rolled back
        Ok(())
    }

    // Set a key-value pair in the storage
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64, Error> {
        if self.in_transaction {
            // If in transaction, log the SET operation in the WAL instead of applying it immediately
            self.wal.log_operation(&WalOperation::Set {
                key: key.to_string(),
                value: value.to_string(),
            })?;
            Ok(0) // Return a temporary offset
        } else {
            let offset = self.segment.set(key, value)?; // Apply the SET operation directly
            self.index.insert(key, offset); // Update the index with the new offset
            Ok(offset)
        }
    }

    // Get the value associated with a key from the storage
    pub fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        if let Some(offset) = self.index.get_offset(key) {
            if let Some(record) = self.segment.get(offset)? {
                return Ok(Some(record.value)); // Return the value if found
            }
        }
        Ok(None) // Return None if the key is not found
    }

    // Delete a key from the storage
    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        if self.in_transaction {
            // If in transaction, log the DELETE operation in the WAL
            self.wal.log_operation(&WalOperation::Delete {
                key: key.to_string(),
            })?;
            Ok(())
        } else {
            if let Some(offset) = self.index.get_offset(key) {
                self.segment.delete(offset)?; // Apply the DELETE operation
                self.index.map.remove(key); // Remove the key from the index
            }
            Ok(())
        }
    }
}
