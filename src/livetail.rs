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
 */

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
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
}

impl LiveTail {
    pub fn new_pipe(&self, id: String, stream: String) -> ReceiverPipe {
        let (sender, revc) = channel(id, stream.clone(), Arc::downgrade(&self.pipes));
        self.pipes
            .write()
            .unwrap()
            .entry(stream)
            .or_default()
            .push(sender);
        revc
    }

    pub fn process(&self, stream_name: &str, rb: &RecordBatch) {
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
}

fn channel(
    id: String,
    stream: String,
    weak_ptr: Weak<LiveTailRegistry>,
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
        if let Some(map) = self._ref.upgrade() {
            if let Some(pipes) = map.write().unwrap().get_mut(&self.stream) {
                pipes.retain(|x| x.id != self.id)
            }
        }
    }
}
