use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, Error, ErrorKind};
use std::path::Path;

pub struct Segment {
    file: File,
    path: String,
    write_offset: u64,
}

impl Segment {
    /// Creates a new Segment by opening or creating the file at the given path.
    /// Sets the initial write offset to the end of the file.
    pub fn new(path: &Path) -> Result<Self, Error> {
        let mut file = OpenOptions::new()
            .create(true)  // Create the file if it doesn't exist
            .append(true)  // Enable appending to the file
            .read(true)    // Enable reading from the file
            .open(path)?;  // Open the file

        // Seek to the end of the file to determine the current write offset
        let write_offset = file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            path: path.display().to_string(),
            write_offset,
        })
    }

    /// Appends a key-value pair to the segment file.
    /// Format: [key_len][value_len][key][value]
    /// Returns the offset before writing, which can be used for indexing.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<u64, Error> {
        // Ensure key and value lengths are within u32 limits
        if key.len() > u32::MAX as usize {
            return Err(Error::new(ErrorKind::InvalidInput, "Key too long"));
        }

        if value.len() > u32::MAX as usize {
            return Err(Error::new(ErrorKind::InvalidInput, "Value too long"));
        }

        // Capture the current write offset before writing
        let offset = self.write_offset;

        let key_len = key.len() as u32;
        let value_len = value.len() as u32;

        // Build the buffer in the format:
        // [key_len: 4 bytes][value_len: 4 bytes][key][value]
        let mut buffer = Vec::with_capacity(8 + key.len() + value.len());
        buffer.extend_from_slice(&key_len.to_le_bytes());    // Write key length (4 bytes)
        buffer.extend_from_slice(&value_len.to_le_bytes());  // Write value length (4 bytes)
        buffer.extend_from_slice(key);                       // Write key bytes
        buffer.extend_from_slice(value);                     // Write value bytes

        // Write the buffer to the file
        self.file.write_all(&buffer)?;

        // Update internal offset after the write
        self.write_offset += buffer.len() as u64;

        // Return the offset prior to writing
        Ok(offset)
    }

    pub fn read_at(&mut self, offset: u64) -> Result<(Vec<u8>, Vec<u8>), Error> {
        // Move the file cursor to the specified offset
        self.file.seek(SeekFrom::Start(offset))?;

        let mut len_buf = [0u8; 4];
        
        // Read the length of the key (4 bytes)
        self.file.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;

        // Read the length of the value (4 bytes)
        self.file.read_exact(&mut len_buf)?;
        let value_len = u32::from_le_bytes(len_buf) as usize;
    

        // Read the key bytes
        let mut key_buf = vec![0u8; key_len];
        self.file.read_exact(&mut key_buf)?;
        
        // Read the value bytes 
        let mut value_buf = vec![0u8; value_len];
        self.file.read_exact(&mut value_buf)?;    
        
        Ok((key_buf, value_buf))
    }
}
