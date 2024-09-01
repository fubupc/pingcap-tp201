use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, Seek},
    path::Path,
};

use failure::Fail;
use serde::{Deserialize, Serialize};
use serde_json::StreamDeserializer;

/// An in-memory key/value store.
pub struct KvStore {
    /// A map of keys to log pointers. When a read request is issued, the in-memory index is searched
    /// for the appropriate log pointer, and when it is found the value is retrieved from the on-disk log.
    /// In our key/value store, like in bitcask, the index for the entire database is stored in memory.
    in_memory_index: HashMap<String, LogPointer>,
    log: Log,
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<KvStore> {
        let mut in_memory_index = HashMap::new();
        for cmd in Log::replay(dir.as_ref())? {
            let cmd = cmd?;
            match cmd.0 {
                Command::Set { key, .. } => {
                    in_memory_index.insert(key, cmd.1);
                }
                Command::Remove { key } => {
                    in_memory_index.remove(&key);
                }
            };
        }

        Ok(KvStore {
            in_memory_index,
            log: Log::open(dir.as_ref())?,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.in_memory_index.get(&key) {
            Some(p) => match self.log.read(p)? {
                Command::Set { value, .. } => Ok(Some(value)),
                Command::Remove { .. } => Err(Error::LogFileCorrupted), // Or maybe replay bug?
            },
            None => Ok(None),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let ptr = self.log.append_command(cmd)?;
        self.in_memory_index.insert(key, ptr);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.in_memory_index
            .remove(&key)
            .ok_or(Error::KeyNotFound)?;
        let cmd = Command::Remove { key: key.clone() };
        self.log.append_command(cmd)?;
        Ok(())
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
pub struct Log {
    file: File,
}

/// A file offset into the log. Sometimes we'll just call this a "file offset".
#[derive(Debug, Clone, Copy)]
pub struct LogPointer(u64);

impl Log {
    fn open<P: AsRef<Path>>(dir: P) -> Result<Log> {
        let path = dir.as_ref().join("kvs.log");
        Ok(Log {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
        })
    }

    fn replay<P: AsRef<Path>>(
        dir: P,
    ) -> Result<impl Iterator<Item = Result<(Command, LogPointer)>>> {
        let path = dir.as_ref().join("kvs.log");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(LogReplay::new(BufReader::new(file)))
    }

    fn read(&mut self, ptr: &LogPointer) -> Result<Command> {
        self.file
            .seek(io::SeekFrom::Start(ptr.0))
            .map_err(|_| Error::LogFileCorrupted)?;
        match serde_json::Deserializer::from_reader(&self.file)
            .into_iter::<Command>()
            .next()
        {
            Some(c) => Ok(c?),
            None => Err(Error::LogFileCorrupted),
        }
    }

    fn append_command(&mut self, cmd: Command) -> Result<LogPointer> {
        let pos = self.file.seek(io::SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.file, &cmd)?;
        Ok(LogPointer(pos))
    }
}

pub struct LogReplay<'de, R: io::Read, T> {
    stream: StreamDeserializer<'de, serde_json::de::IoRead<R>, T>,
}

impl<'de, R> LogReplay<'de, R, Command>
where
    R: io::Read,
{
    fn new(r: R) -> Self {
        Self {
            stream: serde_json::Deserializer::from_reader(r).into_iter::<Command>(),
        }
    }
}

impl<'de, R> Iterator for LogReplay<'de, R, Command>
where
    R: io::Read,
{
    type Item = Result<(Command, LogPointer)>;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.stream.byte_offset() as u64;
        match self.stream.next()? {
            Ok(cmd) => Some(Ok((cmd, LogPointer(pos)))),
            Err(e) => Some(Err(e.into())),
        }
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
