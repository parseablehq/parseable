// use arrow::datatypes::Schema;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Copy, Clone)]
pub struct Stream;

impl Stream {
    pub fn empty() -> Stream{
        // Stream{schema: Schema::empty()}
        Stream
    }
}

lazy_static! {
    pub static ref STREAMS: Mutex<HashMap<String, Stream>> = {
        let map = HashMap::new();
        Mutex::new(map)
    };
}

pub fn insert_stream(str_name: String, str: Stream) -> Option<Stream> {
    let mut map = STREAMS.lock().unwrap();
    map.insert(str_name, str)
}

// pub fn contains_stream(str_name: String) -> bool {
//     let map = STREAMS.lock().unwrap();
//     map.contains_key(&str_name)
// }

