use arrow::record_batch::RecordBatch;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct Stream {
    pub rb: Option<RecordBatch>,
    pub stream_schema: Option<String>,
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
        let init_event = map.get(&stream_name).unwrap();
        drop(&map);
        return init_event.rb.as_ref().unwrap().clone();
    }

    pub fn get_schema(stream_name: String) -> String {
        let map = MEM_STREAMS.lock().unwrap();
        let init_event = map.get(&stream_name).unwrap();
        drop(&map);
        return init_event.stream_schema.as_ref().unwrap().clone();
    }

    pub fn put(stream_name: String, stream: Stream) {
        let mut map = MEM_STREAMS.lock().unwrap();
        map.insert(
            stream_name.to_string(),
            Box::new(Stream {
                stream_schema: Some(stream.stream_schema.unwrap()),
                rb: Some(stream.rb.unwrap()),
            }),
        );
        println!("{:?}", map);
        drop(map);
    }
}
