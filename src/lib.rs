use std::{io, path::Path};

use failure::Fail;

/// An in-memory key/value store.
pub struct KvStore {}

impl KvStore {
    pub fn open<P: AsRef<Path>>(log_file: P) -> Result<KvStore> {
        unimplemented!()
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        unimplemented!()
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        unimplemented!()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] io::Error),
}

pub type Result<T> = ::std::result::Result<T, Error>;
