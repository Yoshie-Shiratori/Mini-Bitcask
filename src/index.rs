use std::collections::HashMap;

pub struct Index {
    map: HashMap<String, u64>
}

impl Index {
    /// Create a new empty index.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Insert a key and its offset into the index.
    pub fn insert(&mut self, key: String, offset: u64) {
        self.map.insert(key, offset);
    }

    /// Returns the offset associated with the given key, if it exists.
    pub fn get_offset(&self, key: &str) -> Option<u64> {
        self.map.get(key).copied()
    }

    // TODO: To be implemented later 
    //pub fn load_from_segment(&mut self, segment: &Segment) -> Result<(), Error> {
    //
    //} 
}
