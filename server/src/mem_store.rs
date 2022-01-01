use arrow::record_batch::RecordBatch;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct Stream {
    pub rb: Option<RecordBatch>,
    pub schema: Option<String>,
}

lazy_static! {
    #[derive(Debug)]
    pub static ref MEM_STREAMS: Mutex<HashMap<String, Box<Stream>>> = {
        let m = HashMap::new();
        Mutex::new(m)
    };
}

impl MEM_STREAMS {
    pub fn get_rb(stream_name: String) -> RecordBatch {
        let map = MEM_STREAMS.lock().unwrap();
        let events = map.get(&stream_name).unwrap();
        drop(&map);
        return events.rb.as_ref().unwrap().clone();
    }

    pub fn get_schema(stream_name: String) -> String {
        let map = MEM_STREAMS.lock().unwrap();
        let events = map.get(&stream_name).unwrap();
        drop(&map);
        return events.schema.as_ref().unwrap().clone();
    }

    pub fn put(stream_name: String, stream: Stream) {
        let mut map = MEM_STREAMS.lock().unwrap();
        map.insert(
            stream_name.to_string(),
            Box::new(Stream {
                schema: Some(stream.schema.unwrap()),
                rb: Some(stream.rb.unwrap()),
            }),
        );
        println!("{:?}", map);
        drop(map);
    }
}
