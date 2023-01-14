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
 */

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub trait Link {
    fn links(&self) -> usize;
    fn increase_link_count(&mut self) -> usize;
    fn decreate_link_count(&mut self) -> usize;
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum CacheState {
    #[default]
    Idle,
    Uploading,
    Uploaded,
}

#[derive(Debug)]
pub struct FileLink {
    link: usize,
    pub metadata: CacheState,
}

impl Default for FileLink {
    fn default() -> Self {
        Self {
            link: 1,
            metadata: CacheState::Idle,
        }
    }
}

impl FileLink {
    pub fn set_metadata(&mut self, state: CacheState) {
        self.metadata = state
    }
}

impl Link for FileLink {
    fn links(&self) -> usize {
        self.link
    }

    fn increase_link_count(&mut self) -> usize {
        self.link.saturating_add(1)
    }

    fn decreate_link_count(&mut self) -> usize {
        self.link.saturating_sub(1)
    }
}

pub struct FileTable<L: Link + Default> {
    inner: HashMap<PathBuf, L>,
}

impl<L: Link + Default> FileTable<L> {
    pub fn new() -> Self {
        Self {
            inner: HashMap::default(),
        }
    }

    pub fn upsert(&mut self, path: &Path) {
        if let Some(entry) = self.inner.get_mut(path) {
            entry.increase_link_count();
        } else {
            self.inner.insert(path.to_path_buf(), L::default());
        }
    }

    pub fn remove(&mut self, path: &Path) {
        let Some(link_count) = self.inner.get_mut(path).map(|entry| entry.decreate_link_count()) else { return };
        if link_count == 0 {
            let _ = std::fs::remove_file(path);
            self.inner.remove(path);
        }
    }

    pub fn get_mut(&mut self, path: &Path) -> Option<&mut L> {
        self.inner.get_mut(path)
    }
}
