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

pub mod reader;
pub mod writer;

#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    #[error("Unable to create recordbatch stream")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("Could not generate parquet file")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("IO Error {0}")]
    ObjectStorage(#[from] std::io::Error),
    #[error("Could not generate parquet file")]
    Create,
    #[error("Could not find stream {0}")]
    NotFound(String),
    // #[error("Metadata Error: {0}")]
    // Metadata(#[from] MetadataError),
}
