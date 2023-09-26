use std::collections::HashMap;

/// An in-memory key/value store.
#[derive(Default)]
pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, k: String) -> Option<String> {
        self.data.get(&k).cloned()
    }

    pub fn set(&mut self, k: String, v: String) -> Option<String> {
        self.data.insert(k, v)
    }

    pub fn remove(&mut self, k: String) -> Option<String> {
        self.data.remove(&k)
    }
}
