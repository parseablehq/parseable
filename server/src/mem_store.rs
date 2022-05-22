/*
 * Parseable Server (C) 2022 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use arrow::record_batch::RecordBatch;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::Error;

#[derive(Debug)]
pub struct LogStream {
    pub rb: Option<RecordBatch>,
    pub schema: Option<String>,
}

lazy_static! {
    #[derive(Debug)]
    pub static ref MEM_STREAMS: Mutex<HashMap<String, Box<LogStream>>> =
        Mutex::new(HashMap::new());
}

#[allow(clippy::all)]
impl MEM_STREAMS {
    pub fn get_rb(stream_name: String) -> Result<RecordBatch, Error> {
        let map = MEM_STREAMS.lock().map_err(|_| Error::StreamLock)?;
        let events = map.get(&stream_name).unwrap();
        drop(&map);

        let record = events.rb.as_ref().unwrap().clone();

        Ok(record)
    }

    pub fn get_schema(stream_name: String) -> String {
        let map = MEM_STREAMS.lock().unwrap();
        let events = map.get(&stream_name).unwrap();
        drop(&map);

        events.schema.as_ref().unwrap().clone()
    }

    pub fn put(stream_name: String, logstream: LogStream) {
        let mut map = MEM_STREAMS.lock().unwrap();
        map.insert(
            stream_name,
            Box::new(LogStream {
                schema: Some(logstream.schema.unwrap()),
                rb: Some(logstream.rb.unwrap()),
            }),
        );
        //println!("{:?}", map);
        drop(map);
    }
}
