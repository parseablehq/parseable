/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
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
 *
 */

use std::{
    fs::{remove_file, File},
    io::BufReader,
    path::PathBuf,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use tracing::error;

use crate::utils::arrow::adapt_batch;

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<StreamReader<BufReader<File>>>,
}

impl MergedRecordReader {
    pub fn try_new(files: &[PathBuf]) -> Self {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            //remove empty files before reading
            if file.metadata().unwrap().len() == 0 {
                error!("Invalid file detected, removing it: {:?}", file);
                remove_file(file).unwrap();
            } else {
                let Ok(reader) =
                    StreamReader::try_new(BufReader::new(File::open(file).unwrap()), None)
                else {
                    error!("Invalid file detected, ignoring it: {:?}", file);
                    continue;
                };

                readers.push(reader);
            }
        }

        Self { readers }
    }

    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.schema().as_ref().clone()),
        )
        .unwrap()
    }

    pub fn merged_iter(self, schema: Arc<Schema>) -> impl Iterator<Item = RecordBatch> {
        self.readers
            .into_iter()
            .flat_map(|reader| reader.flatten())
            .map(move |batch| adapt_batch(&schema, &batch))
    }
}
