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

//! The global allocator of the library's test binary. It forwards to the system
//! allocator and, on a thread that asked for it, counts the bytes that thread
//! allocates, so a test can assert how much memory a call needs instead of
//! inferring it. Other threads (other tests) are never counted. Sizes are
//! layout sizes, not what the system allocator rounds them up to, and a
//! reallocation counts the old and the new block together at its peak.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Debug, Clone, Copy)]
struct Counter {
    live: isize,
    peak: isize,
}

std::thread_local! {
    static COUNTER: Cell<Option<Counter>> = const { Cell::new(None) };
}

fn add(bytes: isize) {
    let _ = COUNTER.try_with(|counter| {
        if let Some(mut c) = counter.get() {
            c.live += bytes;
            c.peak = c.peak.max(c.live);
            counter.set(Some(c));
        }
    });
}

struct Tracking;

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            add(layout.size() as isize);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        add(-(layout.size() as isize));
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // A move holds the old and the new block at once, so the peak counts both
        add(size as isize);
        let result = unsafe { System.realloc(ptr, layout, size) };
        if result.is_null() {
            add(-(size as isize));
        } else {
            add(-(layout.size() as isize));
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

/// What a call allocated on the calling thread
#[derive(Debug, Clone, Copy)]
pub struct Allocated {
    /// Bytes still allocated when the call returned, its result included
    pub retained: isize,
    /// The most bytes the call held at once, above what was allocated before it
    pub peak: usize,
}

/// Runs `f` on this thread and reports what it allocated. Not reentrant.
pub fn measure<T>(f: impl FnOnce() -> T) -> (T, Allocated) {
    COUNTER.with(|c| c.set(Some(Counter { live: 0, peak: 0 })));
    let value = f();
    let counter = COUNTER.with(|c| c.replace(None)).expect("measuring");
    (
        value,
        Allocated {
            retained: counter.live,
            peak: counter.peak.max(0) as usize,
        },
    )
}

/// Bytes `f` still holds allocated when it returns, its result included
pub fn retained<T>(f: impl FnOnce() -> T) -> (T, isize) {
    let (value, allocated) = measure(f);
    (value, allocated.retained)
}
