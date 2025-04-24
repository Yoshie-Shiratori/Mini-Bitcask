use crate::index::Index;
use crate::segment::{Segment, WalOperation, WalSegment};
use std::io::{Error, ErrorKind};
use std::path::Path;

pub struct Storage {
    segment: Segment,
    index: Index,
    wal: WalSegment,
    in_transaction: bool,
}

impl Storage {
    pub fn new(path: &Path) -> Result<Self, Error> {
        let segment = Segment::new(path)?;
        let index = Index::new();
        let wal_path = path.with_extension("wal");
        let wal = WalSegment::new(&wal_path)?;
        let mut storage = Self {
            segment,
            index,
            wal,
            in_transaction: false,
        };

        let operations = storage.wal.read_operations()?;
        if !operations.is_empty() {
            for op in operations {
                match op {
                    WalOperation::Set { key, value } => {
                        let offset = storage.segment.set(&key, &value)?;
                        storage.index.insert(&key, offset);
                    }
                    WalOperation::Delete { key } => {
                        if let Some(offset) = storage.index.get_offset(&key) {
                            storage.segment.delete(offset)?;
                            storage.index.map.remove(&key);
                        }
                    }
                }
            }
            storage.wal.clear()?;
        }

        Ok(storage)
    }

    pub fn begin_transaction(&mut self) -> Result<(), Error> {
        if self.in_transaction {
            return Err(Error::new(
                ErrorKind::Other,
                "Transaction already in progress",
            ));
        }
        self.in_transaction = true;
        self.wal.clear()?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        if !self.in_transaction {
            return Err(Error::new(ErrorKind::Other, "No active transaction"));
        }
        let operations = self.wal.read_operations()?;
        for op in operations {
            match op {
                WalOperation::Set { key, value } => {
                    let offset = self.segment.set(&key, &value)?;
                    self.index.insert(&key, offset);
                }
                WalOperation::Delete { key } => {
                    if let Some(offset) = self.index.get_offset(&key) {
                        self.segment.delete(offset)?;
                        self.index.map.remove(&key);
                    }
                }
            }
        }
        self.wal.clear()?;
        self.in_transaction = false;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), Error> {
        self.wal.clear()?;
        self.in_transaction = false;
        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<u64, Error> {
        if self.in_transaction {
            self.wal.log_operation(&WalOperation::Set {
                key: key.to_string(),
                value: value.to_string(),
            })?;
            Ok(0) // Temporary offset
        } else {
            let offset = self.segment.set(key, value)?;
            self.index.insert(key, offset);
            Ok(offset)
        }
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>, Error> {
        if let Some(offset) = self.index.get_offset(key) {
            if let Some(record) = self.segment.get(offset)? {
                return Ok(Some(record.value));
            }
        }
        Ok(None)
    }

    pub fn delete(&mut self, key: &str) -> Result<(), Error> {
        if self.in_transaction {
            self.wal.log_operation(&WalOperation::Delete {
                key: key.to_string(),
            })?;
            Ok(())
        } else {
            if let Some(offset) = self.index.get_offset(key) {
                self.segment.delete(offset)?;
                self.index.map.remove(key);
            }
            Ok(())
        }
    }
}
