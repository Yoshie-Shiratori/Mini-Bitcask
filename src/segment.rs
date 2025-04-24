use serde::{Serialize, Deserialize};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read, Error, ErrorKind};
use std::path::Path;
use bincode::{config, Decode, Encode};
use bincode::config::Configuration;

const BINCODE_CONFIG: Configuration = config::standard();

#[derive(Serialize, Deserialize, Debug, PartialEq, Encode, Decode)]
pub struct Record {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Encode, Decode)]
pub enum WalOperation {
    Set { key: String, value: String },
    Delete { key: String },
}

pub struct Segment {
    file: File,
    write_offset: u64,
}

impl Segment {
    pub fn new(path: &Path) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        
        let write_offset = file.metadata()?.len();
        
        Ok(Self { file, write_offset })
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<u64, Error> {
        let record = Record {
            key: key.to_string(),
            value: value.to_string(),
        };

        let serialized = bincode::encode_to_vec(&record, BINCODE_CONFIG)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let offset = self.write_offset;
        
        let size = serialized.len() as u32;
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&serialized)?;
        self.write_offset += 4 + serialized.len() as u64;

        Ok(offset)
    }

    pub fn get(&mut self, offset: u64) -> Result<Option<Record>, Error> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mut size_buf = [0u8; 4];
        self.file.read_exact(&mut size_buf)?;
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut buffer = vec![0u8; size];
        self.file.read_exact(&mut buffer)?;

        match bincode::decode_from_slice(&buffer, BINCODE_CONFIG) {
            Ok((record, _)) => Ok(Some(record)),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e)),
        }
    }

    pub fn delete(&mut self, offset: u64) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(offset))?;

        let deleted_record = Record {
            key: String::from("deleted"),
            value: String::from(""),
        };

        let serialized = bincode::encode_to_vec(&deleted_record, BINCODE_CONFIG)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let size = serialized.len() as u32;
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&serialized)?;

        Ok(())
    }
}

pub struct WalSegment {
    file: File,
}

impl WalSegment {
    pub fn new(path: &Path) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn log_operation(&mut self, op: &WalOperation) -> Result<(), Error> {
        let serialized = bincode::encode_to_vec(op, BINCODE_CONFIG)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let size = serialized.len() as u32;
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&serialized)?;
        Ok(())
    }

    pub fn read_operations(&mut self) -> Result<Vec<WalOperation>, Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut operations = Vec::new();

        loop {
            let mut size_buf = [0u8; 4];
            match self.file.read_exact(&mut size_buf) {
                Ok(_) => {
                    let size = u32::from_le_bytes(size_buf) as usize;
                    let mut buffer = vec![0u8; size];
                    self.file.read_exact(&mut buffer)?;
                    let (op, _) = bincode::decode_from_slice(&buffer, BINCODE_CONFIG)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                    operations.push(op);
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(operations)
    }

    pub fn clear(&mut self) -> Result<(), Error> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}
