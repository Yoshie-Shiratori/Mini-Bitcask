use bincode::config::Configuration;
use bincode::{Decode, Encode, config};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

const BINCODE_CONFIG: Configuration = config::standard(); // Standard bincode configuration

/// Structure representing a record in the storage (key-value pair)
#[derive(Serialize, Deserialize, Debug, PartialEq, Encode, Decode)]
pub struct Record {
    pub key: String,   // Key of the record
    pub value: String, // Value associated with the key
}

/// Enum representing the operations that can be logged in the WAL (Write-Ahead Log)
#[derive(Serialize, Deserialize, Debug, PartialEq, Encode, Decode)]
pub enum WalOperation {
    Set { key: String, value: String }, // Set operation (key-value pair)
    Delete { key: String },             // Delete operation (by key)
}

/// Structure representing a segment (a file) where records are stored
pub struct Segment {
    file: File,        // File where the records are written
    write_offset: u64, // Current position to write in the file
}

impl Segment {
    /// Creates a new segment by opening or creating the file at the given path
    pub fn new(path: &Path) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true) // Create the file if it doesn't exist
            .append(true) // Allow appending data to the file
            .read(true) // Allow reading from the file
            .open(path)?; // Open the file at the given path

        let write_offset = file.metadata()?.len(); // Get the current length of the file (write position)

        Ok(Self { file, write_offset }) // Return the segment with the file and write offset
    }

    /// Set a key-value pair in the segment and returns the offset where it was written
    pub fn set(&mut self, key: &str, value: &str) -> Result<u64, Error> {
        let record = Record {
            key: key.to_string(),
            value: value.to_string(),
        };

        // Serialize the record using bincode
        let serialized = bincode::encode_to_vec(&record, BINCODE_CONFIG)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let offset = self.write_offset; // The current position in the file

        let size = serialized.len() as u32;
        // Write the size of the record followed by the record itself
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&serialized)?;
        self.write_offset += 4 + serialized.len() as u64; // Update the write offset

        Ok(offset)
    }

    /// Get a record from the segment by its offset
    pub fn get(&mut self, offset: u64) -> Result<Option<Record>, Error> {
        self.file.seek(SeekFrom::Start(offset))?; // Move to the specified offset

        let mut size_buf = [0u8; 4];
        self.file.read_exact(&mut size_buf)?; // Read the size of the record
        let size = u32::from_le_bytes(size_buf) as usize;

        let mut buffer = vec![0u8; size];
        self.file.read_exact(&mut buffer)?; // Read the serialized record

        match bincode::decode_from_slice(&buffer, BINCODE_CONFIG) {
            Ok((record, _)) => Ok(Some(record)), // Successfully decoded the record
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e)), // Error while decoding
        }
    }

    /// Mark a record as deleted at the given offset
    pub fn delete(&mut self, offset: u64) -> Result<(), Error> {
        self.file.seek(SeekFrom::Start(offset))?; // Move to the specified offset

        // Create a "deleted" record with an empty value
        let deleted_record = Record {
            key: String::from("deleted"),
            value: String::from(""),
        };

        let serialized = bincode::encode_to_vec(&deleted_record, BINCODE_CONFIG)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let size = serialized.len() as u32;
        // Write the size of the "deleted" record followed by the record itself
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&serialized)?;

        Ok(())
    }
}

/// Structure representing the WAL (Write-Ahead Log) segment (a file where operations are logged)
pub struct WalSegment {
    file: File, // File where the operations are logged
}

impl WalSegment {
    /// Creates a new WAL segment by opening or creating the file at the given path
    pub fn new(path: &Path) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true) // Create the file if it doesn't exist
            .read(true) // Allow reading from the file
            .write(true) // Allow writing to the file
            .open(path)?; // Open the file at the given path
        Ok(Self { file }) // Return the WAL segment
    }

    /// Log a WAL operation (Set or Delete) to the WAL file
    pub fn log_operation(&mut self, op: &WalOperation) -> Result<(), Error> {
        let serialized = bincode::encode_to_vec(op, BINCODE_CONFIG)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let size = serialized.len() as u32;
        // Write the size of the operation followed by the serialized operation
        self.file.write_all(&size.to_le_bytes())?;
        self.file.write_all(&serialized)?;
        Ok(())
    }

    /// Read all operations from the WAL file and return them as a vector
    pub fn read_operations(&mut self) -> Result<Vec<WalOperation>, Error> {
        self.file.seek(SeekFrom::Start(0))?; // Start reading from the beginning of the file
        let mut operations = Vec::new();

        loop {
            let mut size_buf = [0u8; 4];
            match self.file.read_exact(&mut size_buf) {
                Ok(_) => {
                    let size = u32::from_le_bytes(size_buf) as usize;
                    let mut buffer = vec![0u8; size];
                    self.file.read_exact(&mut buffer)?; // Read the serialized operation
                    let (op, _) = bincode::decode_from_slice(&buffer, BINCODE_CONFIG)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                    operations.push(op); // Add the operation to the list
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break, // Break if EOF is reached
                Err(e) => return Err(e),                                 // Propagate other errors
            }
        }

        Ok(operations)
    }

    /// Clear the WAL file (reset it)
    pub fn clear(&mut self) -> Result<(), Error> {
        self.file.set_len(0)?; // Truncate the file to 0 length
        self.file.seek(SeekFrom::Start(0))?; // Move the cursor to the beginning of the file
        Ok(())
    }
}
