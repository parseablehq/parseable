/*
 * Parseable Server (C) 2022 - 2023 Parseable, Inc.
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

use std::{fs::File, path::PathBuf};

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use itertools::kmerge_by;

use super::adapt_batch;

#[derive(Debug)]
pub struct Reader {
    reader: StreamReader<File>,
    timestamp_col_index: usize,
}

impl From<StreamReader<File>> for Reader {
    fn from(reader: StreamReader<File>) -> Self {
        let timestamp_col_index = reader
            .schema()
            .all_fields()
            .binary_search_by(|field| field.name().as_str().cmp("p_timestamp"))
            .expect("schema should have this field");

        Self {
            reader,
            timestamp_col_index,
        }
    }
}

#[derive(Debug)]
pub struct MergedRecordReader {
    pub readers: Vec<Reader>,
}

impl MergedRecordReader {
    pub fn try_new(files: &[PathBuf]) -> Result<Self, ()> {
        let mut readers = Vec::with_capacity(files.len());

        for file in files {
            let reader = StreamReader::try_new(File::open(file).unwrap(), None).map_err(|_| ())?;
            readers.push(reader.into());
        }

        Ok(Self { readers })
    }

    pub fn merged_iter(self, schema: &Schema) -> impl Iterator<Item = RecordBatch> + '_ {
        let adapted_readers = self.readers.into_iter().map(move |reader| {
            reader
                .reader
                .flatten()
                .zip(std::iter::repeat(reader.timestamp_col_index))
        });

        kmerge_by(
            adapted_readers,
            |(a, a_col): &(RecordBatch, usize), (b, b_col): &(RecordBatch, usize)| {
                let a: &TimestampMillisecondArray = a
                    .column(*a_col)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();

                let b: &TimestampMillisecondArray = b
                    .column(*b_col)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();

                a.value(0) < b.value(0)
            },
        )
        .map(|(batch, _)| adapt_batch(schema, batch))
    }

    pub fn merged_schema(&self) -> Schema {
        Schema::try_merge(
            self.readers
                .iter()
                .map(|reader| reader.reader.schema().as_ref().clone()),
        )
        .unwrap()
    }
}
