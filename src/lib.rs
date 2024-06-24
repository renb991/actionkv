use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use serde::de;
use serde_derive::{Deserialize, Serialize};

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString
}

#[derive(Debug)]
pub struct ActionKV {
    f: File,
    pub index: HashMap<ByteString, u64>
}

impl ActionKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new().read(true).write(true).create(true).append(true).open(path).unwrap();
        let index = HashMap::new();
        Ok(ActionKV {f, index} )
    }

    fn process_record<R: Read>(r: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = r.read_u32::<LittleEndian>()?;
        let key_len = r.read_u32::<LittleEndian>()?;
        let value_len = r.read_u32::<LittleEndian>()?;
        let data_len = key_len + value_len;

        let mut data = ByteString::with_capacity(data_len as usize);

        r.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        debug_assert_eq!(data.len(), data_len  as usize);

        let checksum = crc32::checksum_ieee(&data);
        println!("expect checksum=>{}, actura=>{}", &saved_checksum, &checksum);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x}) ({:08x})", checksum, saved_checksum
            );
        }
        let value = data.split_off(key_len as usize);
        let key = data;
        Ok(KeyValuePair{key, value})
    }

    fn seek_to_end(&mut self) -> io::Result<u64>{
        self.f.seek(SeekFrom::End(0))
    }

    pub fn load(&mut self) -> io::Result<()>{
        let mut f = BufReader::new(&mut self.f);
        loop{
            let current_pos = f.seek(SeekFrom::Current((0)))?;
            let maybe_kv = ActionKV::process_record(&mut f);
            let result_kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind(){
                        io::ErrorKind::UnexpectedEof => {
                            break
                        },
                        _ => return Err(err),
                    }
                }
            };
            self.index.insert(result_kv.key, current_pos);
        }
        Ok(())
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let pos = match self.index.get(key) {
            None => return Ok(None),
            Some(pos) => *pos,
        };
        let kv = self.get_at(pos)?;
        Ok(Some(kv.value))
    }

    fn get_at(&mut self, pos: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(pos));
        Ok(ActionKV::process_record(&mut f)?)
    }

    fn find(&mut self, target: &ByteStr) -> io::Result<Option<KeyValuePair>> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Current(0));
        loop {
            let current = ActionKV::process_record(&mut f);
            let kv = match current {
                Ok(kv) => {
                    if kv.value == *target {
                        return Ok(Some(kv));
                    }
                    break;
                },
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => return Ok(None),
                        _ => return Err(err),
                    }
                }
            };
            
        }
        Ok(None)
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()>{
        let pos = self.insert_but_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), pos);
        Ok(())
    }

    fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64>{
        let mut f = BufWriter::new(&mut self.f);
        let key_len = key.len();
        let value_len = value.len();
        let mut data = ByteString::with_capacity(key_len + value_len);
        for byte in key {
            data.push(*byte);
        }
        for byte in value {
            data.push(*byte);
        }
        let check_sum = crc32::checksum_ieee(&data);

        let current_pos = f.seek(SeekFrom::End(0))?;
        f.write_u32::<LittleEndian>(check_sum);
        f.write_u32::<LittleEndian>(key_len as u32);
        f.write_u32::<LittleEndian>(value_len as u32);
        f.write_all(&data);
        Ok(current_pos)
    }

    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()>{
        self.insert(key, value)
    }

    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    
    }

}