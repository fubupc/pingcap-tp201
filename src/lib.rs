use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, Seek},
    iter,
    path::Path,
};

use either::Either;
use failure::Fail;
use serde::{Deserialize, Serialize};

/// An in-memory key/value store.
pub struct KvStore {
    data: HashMap<String, String>,
    log: Log,
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<KvStore> {
        let mut data = HashMap::new();
        for cmd in Log::read_log(dir.as_ref()) {
            match cmd? {
                Command::Set { key, value } => {
                    data.insert(key, value);
                }
                Command::Remove { key } => {
                    data.remove(&key);
                }
            };
        }

        Ok(KvStore {
            data,
            log: Log::open(dir.as_ref())?,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.data.get(&key) {
            Some(v) => Ok(Some(v.clone())),
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.log.append_command(cmd)?;
        self.data.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key: key.clone() };
        self.log.append_command(cmd)?;
        self.data.remove(&key).map(|_| ()).ok_or(Error::KeyNotFound)
    }
}

/// A request or the representation of a request made to the database. These are issued on the command line
/// or over the network. They have an in-memory representation, a textual representation, and a machine-readable
/// serialized representation.
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

/// An on-disk sequence of commands, in the order originally received and executed. Our database's on-disk format is
/// almost entirely made up of logs. It will be simple, but also surprisingly efficient.
struct Log {
    file: File,
}

impl Log {
    fn open<P: AsRef<Path>>(dir: P) -> Result<Log> {
        let path = dir.as_ref().join("kvs.log");
        Ok(Log {
            file: OpenOptions::new().append(true).create(true).open(path)?,
        })
    }

    fn read_log<P: AsRef<Path>>(dir: P) -> impl Iterator<Item = Result<Command>> {
        let path = dir.as_ref().join("kvs.log");
        match File::open(path) {
            Ok(f) => {
                let buf = BufReader::new(f);
                Either::Right(
                    serde_json::Deserializer::from_reader(buf)
                        .into_iter::<Command>()
                        .map(|c| c.map_err(|e| e.into())),
                )
            }
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Either::Left(Either::Right(iter::empty())),
                _ => Either::Left(Either::Left(iter::once(Err(e.into())))),
            },
        }
    }

    fn append_command(&mut self, cmd: Command) -> Result<()> {
        self.file.seek(io::SeekFrom::End(0))?;
        Ok(serde_json::to_writer(&mut self.file, &cmd)?)
    }
}

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "Serde error: {}", _0)]
    Serde(#[cause] serde_json::Error),

    #[fail(display = "Log file corrupted")]
    LogFileCorrupted,

    #[fail(display = "Key not found")]
    KeyNotFound,
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value)
    }
}
