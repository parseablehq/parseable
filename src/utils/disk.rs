/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
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

use std::path::Path;

use sysinfo::Disks;

#[derive(Clone, Copy)]
pub(crate) struct DiskUtil {
    pub(crate) total_space: u64,
    pub(crate) available_space: u64,
    pub(crate) used_space: u64,
}

pub(crate) fn disk_usage_for_path(path: &Path) -> Option<DiskUtil> {
    let mut disks = Disks::new_with_refreshed_list();
    // Prefer the most specific mount point containing the requested path.
    disks.sort_by_key(|disk| disk.mount_point().as_os_str().len());
    disks.reverse();

    disks.iter().find_map(|disk| {
        path.starts_with(disk.mount_point()).then(|| DiskUtil {
            total_space: disk.total_space(),
            available_space: disk.available_space(),
            used_space: disk.total_space() - disk.available_space(),
        })
    })
}
