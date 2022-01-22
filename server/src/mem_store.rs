/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

#[allow(clippy::all)]
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
            stream_name,
            Box::new(Stream {
                schema: Some(stream.schema.unwrap()),
                rb: Some(stream.rb.unwrap()),
            }),
        );
        //println!("{:?}", map);
        drop(map);
    }
}
