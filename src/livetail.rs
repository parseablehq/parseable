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

use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use futures_util::Stream;
use tokio::sync::mpsc::{
    self, Receiver, Sender, UnboundedReceiver, UnboundedSender, error::TrySendError,
};

use arrow_array::RecordBatch;
use once_cell::sync::Lazy;

pub static LIVETAIL: Lazy<LiveTail> = Lazy::new(LiveTail::default);

pub type LiveTailRegistry = RwLock<HashMap<String, Vec<SenderPipe>>>;

pub struct LiveTail {
    pipes: Arc<LiveTailRegistry>,
    /// Number of sender pipes in `pipes`, across all streams. Only changed
    /// while holding the registry's write lock, so it equals the registry
    /// size whenever no writer is active. Lets the per-row ingest path skip
    /// the registry lock and the stream-name hash when nobody is tailing.
    active_pipes: Arc<AtomicUsize>,
}

impl LiveTail {
    pub fn new_pipe(&self, id: String, stream: String) -> ReceiverPipe {
        let (sender, revc) = channel(
            id,
            stream.clone(),
            Arc::downgrade(&self.pipes),
            Arc::clone(&self.active_pipes),
        );
        let mut pipes = self.pipes.write().unwrap();
        pipes.entry(stream).or_default().push(sender);
        self.active_pipes.fetch_add(1, Ordering::Relaxed);
        revc
    }

    /// Whether any live-tail pipe is registered for any stream.
    #[inline]
    fn any_subscribers(&self) -> bool {
        self.active_pipes.load(Ordering::Relaxed) != 0
    }

    #[cfg(test)]
    pub(crate) fn active_pipes(&self) -> usize {
        self.active_pipes.load(Ordering::Relaxed)
    }

    pub(crate) fn has_subscribers(&self, stream_name: &str) -> bool {
        if !self.any_subscribers() {
            return false;
        }
        self.pipes
            .read()
            .unwrap()
            .get(stream_name)
            .is_some_and(|pipes| !pipes.is_empty())
    }

    pub(crate) fn process_row(&self, stream_name: &str, rb: &RecordBatch, offset: usize) {
        if !self.any_subscribers() {
            return;
        }
        let read = self.pipes.read().unwrap();
        if let Some(pipes) = read.get(stream_name) {
            for pipe in pipes {
                pipe.send(rb.slice(offset, 1));
            }
        }
    }

    pub fn process(&self, stream_name: &str, rb: &RecordBatch) {
        if !self.any_subscribers() {
            return;
        }
        let read = self.pipes.read().unwrap();
        let Some(pipes) = read.get(stream_name) else {
            return;
        };
        for pipe in pipes {
            pipe.send(rb.clone())
        }
    }
}

impl Default for LiveTail {
    fn default() -> Self {
        Self {
            pipes: Arc::new(RwLock::default()),
            active_pipes: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Message {
    Record(RecordBatch),
    Skipped(usize),
}

// Receiver should swap out channel for a new one when full
pub enum Command {
    Skipping(usize),
}

type Id = String;
pub struct SenderPipe {
    pub id: Id,
    inner: Sender<RecordBatch>,
    command: UnboundedSender<Command>,
}

impl SenderPipe {
    pub fn send(&self, rb: RecordBatch) {
        if let Err(TrySendError::Full(rb)) = self.inner.try_send(rb) {
            self.command
                .send(Command::Skipping(rb.num_rows()))
                .expect("receiver is not dropped before sender")
        }
    }
}

pub struct ReceiverPipe {
    pub id: Id,
    pub stream: String,
    inner: Receiver<RecordBatch>,
    command: UnboundedReceiver<Command>,
    _ref: Weak<LiveTailRegistry>,
    active_pipes: Arc<AtomicUsize>,
}

fn channel(
    id: String,
    stream: String,
    weak_ptr: Weak<LiveTailRegistry>,
    active_pipes: Arc<AtomicUsize>,
) -> (SenderPipe, ReceiverPipe) {
    let (command_tx, command_rx) = mpsc::unbounded_channel::<Command>();
    let (rb_tx, rb_rx) = mpsc::channel::<RecordBatch>(1000);

    (
        SenderPipe {
            id: id.clone(),
            inner: rb_tx,
            command: command_tx,
        },
        ReceiverPipe {
            id,
            stream,
            _ref: weak_ptr,
            active_pipes,
            inner: rb_rx,
            command: command_rx,
        },
    )
}

impl Stream for ReceiverPipe {
    type Item = Message;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.command.poll_recv(cx) {
            Poll::Ready(Some(Command::Skipping(mut row_count))) => {
                while let Poll::Ready(Some(rb)) = this.inner.poll_recv(cx) {
                    row_count += rb.num_rows();
                }
                while let Poll::Ready(Some(Command::Skipping(count))) = this.command.poll_recv(cx) {
                    row_count += count
                }
                Poll::Ready(Some(Message::Skipped(row_count)))
            }
            Poll::Pending => match this.inner.poll_recv(cx) {
                Poll::Ready(Some(rb)) => Poll::Ready(Some(Message::Record(rb))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            },
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

// drop sender on map when going out of scope
impl Drop for ReceiverPipe {
    fn drop(&mut self) {
        if let Some(map) = self._ref.upgrade()
            && let Ok(mut guard) = map.write()
            && let Some(pipes) = guard.get_mut(&self.stream)
        {
            let before = pipes.len();
            pipes.retain(|x| x.id != self.id);
            let removed = before - pipes.len();
            if pipes.is_empty() {
                guard.remove(&self.stream);
            }
            // Updated under the write lock, like the insert in `new_pipe`.
            self.active_pipes.fetch_sub(removed, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array};
    use futures_util::StreamExt;

    fn batch(values: &[i64]) -> RecordBatch {
        let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_from_iter([("v", column)]).unwrap()
    }

    #[tokio::test]
    async fn active_pipe_counter_tracks_registry_and_gates_delivery() {
        let tail = LiveTail::default();
        assert_eq!(tail.active_pipes(), 0);
        assert!(!tail.has_subscribers("a"));
        // No subscribers: fast exit, nothing to deliver.
        tail.process_row("a", &batch(&[1]), 0);
        tail.process("a", &batch(&[1]));

        let mut first = tail.new_pipe("1".into(), "a".into());
        let second = tail.new_pipe("2".into(), "a".into());
        let other = tail.new_pipe("3".into(), "b".into());
        assert_eq!(tail.active_pipes(), 3);
        assert!(tail.has_subscribers("a"));
        assert!(!tail.has_subscribers("c"));

        let rb = batch(&[10, 20, 30]);
        tail.process_row("a", &rb, 1);
        assert_eq!(first.next().await, Some(Message::Record(rb.slice(1, 1))));

        drop(second);
        assert_eq!(tail.active_pipes(), 2);
        assert!(tail.has_subscribers("a"));
        drop(first);
        assert_eq!(tail.active_pipes(), 1);
        assert!(!tail.has_subscribers("a"));
        assert!(tail.pipes.read().unwrap().get("a").is_none());
        drop(other);
        assert_eq!(tail.active_pipes(), 0);
        assert!(tail.pipes.read().unwrap().is_empty());

        // A pipe registered after the counter returned to zero still receives.
        let mut again = tail.new_pipe("4".into(), "a".into());
        assert_eq!(tail.active_pipes(), 1);
        tail.process("a", &rb);
        assert_eq!(again.next().await, Some(Message::Record(rb.clone())));
    }

    #[test]
    fn dropping_receiver_after_registry_is_gone_does_not_underflow() {
        let tail = LiveTail::default();
        let receiver = tail.new_pipe("1".into(), "a".into());
        let counter = Arc::clone(&tail.active_pipes);
        drop(tail);
        drop(receiver);
        // The registry is gone, so nothing was removed and the count is unchanged.
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
