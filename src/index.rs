use std::collections::HashMap;

pub struct Index {
    pub map: HashMap<String, u64>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, offset: u64) {
        self.map.insert(key.to_string(), offset);
    }

    pub fn get_offset(&self, key: &str) -> Option<u64> {
        self.map.get(key).cloned()
    }
}
