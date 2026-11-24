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

pub mod decoded_size;
pub mod logs;
pub mod metrics;
pub mod otel_utils;
pub mod protobuf_stream;
pub mod sink;
pub mod traces;

#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod realistic_tests;
#[cfg(test)]
mod regression_tests;

/// Shared driver for the release-profile micro-benchmarks. Each benchmark is an
/// `#[ignore]`d test that never touches the `PARSEABLE` global, so it can be run
/// with a test-name filter:
///
/// ```text
/// cargo test --release --lib -- --ignored --exact otel::logs::tests::bench_flatten_otel_protobuf_logs --nocapture
/// ```
///
/// Every iteration must flatten the same input to the same rows, so a digest of
/// the rows is printed (and, with `OTEL_BENCH_DUMP=<dir>`, written out) next to
/// the per-row wall time for `perf stat` to turn into instruction counts.
#[cfg(test)]
pub(crate) mod bench {
    use crate::utils::json::flatten::JsonFlattenError;
    use std::time::Instant;

    /// Runs `body` `iters` times, requiring identical output every time, and
    /// prints the rows, their digest and the nanoseconds spent per row.
    ///
    /// `OTEL_BENCH_ITERS=<n>` overrides `iters`. Running a benchmark at two
    /// iteration counts under `perf stat` and dividing the difference in
    /// instructions by the difference in rows flattened removes the fixed cost
    /// of building the request and taking the digests.
    pub fn run<F>(name: &str, iters: usize, mut body: F)
    where
        F: FnMut() -> Vec<serde_json::Value>,
    {
        let iters = std::env::var("OTEL_BENCH_ITERS")
            .ok()
            .and_then(|iters| iters.parse().ok())
            .unwrap_or(iters);
        // Serialising the rows costs more than flattening them, so the digest is
        // taken before and after the timed loop, never inside it.
        let first = body();
        let rows = first.len();
        let expected = rows_digest(&first);
        if let Some(dir) = dump_dir() {
            std::fs::write(
                format!("{dir}/{name}.json"),
                serde_json::to_vec(&first).expect("benchmark rows serialise"),
            )
            .expect("benchmark rows dump");
        }

        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(body());
        }
        let elapsed = start.elapsed();

        let last = body();
        assert_eq!(
            rows_digest(&last),
            expected,
            "{name}: repeated iterations produced different rows"
        );
        println!(
            "{name}: rows={rows} iters={iters} ns_per_row={:.1} bytes={} digest={:016x}",
            elapsed.as_secs_f64() * 1e9 / (rows * iters.max(1)) as f64,
            expected.0,
            expected.1
        );
    }

    fn rows_digest(rows: &[serde_json::Value]) -> (usize, u64) {
        use std::hash::Hasher;
        let bytes = serde_json::to_vec(rows).expect("benchmark rows serialise");
        let mut hasher = rustc_hash::FxHasher::default();
        hasher.write(&bytes);
        (bytes.len(), hasher.finish())
    }

    /// Times `iters` repetitions of `body` and prints the nanoseconds per call.
    pub fn time_calls<F: FnMut(usize)>(name: &str, iters: usize, mut body: F) {
        let start = Instant::now();
        for i in 0..iters {
            body(i);
        }
        println!(
            "{name}: iters={iters} ns_per_call={:.1}",
            start.elapsed().as_secs_f64() * 1e9 / iters as f64
        );
    }

    /// The rows `flatten` hands to a `Chunker` with the default limits, each
    /// chunk appended to the result as it is passed on
    pub fn chunked(
        flatten: impl FnOnce(
            &mut super::sink::Chunker<
                &mut dyn FnMut(Vec<serde_json::Value>, usize) -> Result<(), JsonFlattenError>,
            >,
        ) -> Result<(), JsonFlattenError>,
    ) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        let mut stage = |chunk: Vec<serde_json::Value>, _bytes: usize| {
            rows.extend(chunk);
            Ok(())
        };
        let mut chunker = super::sink::Chunker::new(
            super::sink::CHUNK_LIMITS,
            &mut stage
                as &mut dyn FnMut(Vec<serde_json::Value>, usize) -> Result<(), JsonFlattenError>,
        );
        flatten(&mut chunker).expect("benchmark rows flatten");
        chunker.finish().expect("benchmark rows stage");
        drop(chunker);
        rows
    }

    fn dump_dir() -> Option<String> {
        std::env::var("OTEL_BENCH_DUMP").ok()
    }
}
