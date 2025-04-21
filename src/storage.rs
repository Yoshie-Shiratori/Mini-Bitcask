use std::path::Path;
use std::io::{Error, ErrorKind};

use crate::segment::Segment;
use crate::index::Index;

/// Storage is the core structure that ties together Segment and Index.
/// It handles reading/writing data and maintaining an in-memory index.
pub struct Storage {
    segment: Segment,
    index: Index,
}

impl Storage {
    /// Creates a new Storage instance with a given file path for the segment.
    pub fn new(path: &Path) -> Result<Self, Error> {
        let segment = Segment::new(path)?;
        let index = Index::new();

        Ok(Self {segment, index})
    }

    /// Writes a key-value pair to the segment and stores its offset in the index.
    pub fn put(&mut self, key: String, value: Vec<u8>) -> Result<(), Error> {
        let offset = self.segment.append(key.as_bytes(), &value)?;
        self.index.insert(key, offset);
        Ok(())
    }

    /// Retrieves a value by key using the index and segment.
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        if let Some(offset) = self.index.get_offset(key) {
            let (stored_key, value) = self.segment.read_at(offset)?;

            if stored_key == key.as_bytes() {
                Ok(Some(value)) 
            } else {
                // This should not happen in practice if the index is consistent
                Err(Error::new(ErrorKind::InvalidData, "Key mismatch in offset"))
            }
        } else {
            Ok(None)
        }
    }
}
