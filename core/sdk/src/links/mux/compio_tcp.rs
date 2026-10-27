// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Owned-buffer TCP tasks sharing the ordinary multiplexed link state.

use super::{LinkTasks, MuxLink, MuxShared, QueuedFrame};
use crate::links::Reply;
use crate::runtime::Runtime;
use bytes::Bytes;
use compio::BufResult;
use compio::buf::{IntoInner, IoBuf};
use compio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use compio::net::TcpStream;
use iggy_binary_protocol::consensus::HEADER_SIZE;
use iggy_common::IggyError;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::error;

/// Chunk for draining a late reply that no caller waits for.
const DRAIN_CHUNK: usize = 16 * 1024;

impl MuxLink {
    pub(crate) fn compio_tcp(stream: TcpStream, peer: SocketAddr) -> Self {
        let (reader, writer) = stream.into_split();
        let (shared, queue, frames) = Self::channel(Runtime::Compio, peer);
        let tasks = LinkTasks {
            writer: Runtime::spawn_compio_local(write(writer, frames, Arc::clone(&shared))),
            reader: Runtime::spawn_compio_local(read(reader, Arc::clone(&shared))),
        };
        Self {
            runtime: Runtime::Compio,
            shared,
            queue,
            tasks,
        }
    }
}

async fn write(
    mut writer: TcpStream,
    mut frames: mpsc::Receiver<QueuedFrame>,
    shared: Arc<MuxShared>,
) {
    while let Some(frame) = shared.next_frame(&mut frames).await {
        let header = Bytes::copy_from_slice(bytemuck::bytes_of(&frame.header));
        // Keep header and body in one vectored operation; the body retains its reservation.
        let BufResult(result, _buffers) = writer.write_vectored_all([header, frame.body]).await;
        if let Err(error) = result {
            error!("Failed to send a request to {}: {error}", shared.peer);
            shared.close(&IggyError::Disconnected);
            return;
        }
    }
    let _ = writer.shutdown().await;
}

async fn read(mut reader: TcpStream, shared: Arc<MuxShared>) {
    let reason = loop {
        match read_reply(&mut reader, &shared).await {
            Ok(reply) => {
                if let Err(reason) = shared.dispatch(reply) {
                    break reason;
                }
            }
            Err(error) => {
                if shared.is_open() {
                    error!("Failed to read a reply from {}: {error}", shared.peer);
                }
                break IggyError::Disconnected;
            }
        }
    };
    shared.close(&reason);
}

async fn read_reply(reader: &mut TcpStream, shared: &MuxShared) -> io::Result<Reply> {
    let BufResult(result, header) = reader.read_exact([0; HEADER_SIZE]).await;
    result?;
    let (size, read) = shared
        .frame_size(&header)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let body_len = size - HEADER_SIZE;
    if !read {
        let mut remaining = body_len;
        while remaining > 0 {
            let take = remaining.min(DRAIN_CHUNK);
            let buffer = Vec::<u8>::with_capacity(take).slice(..take);
            let BufResult(result, _) = reader.read_exact(buffer).await;
            result?;
            remaining -= take;
        }
        return Ok(Reply {
            header,
            body: Bytes::new(),
        });
    }
    let body = Vec::<u8>::with_capacity(body_len).slice(..body_len);
    let BufResult(result, body) = reader.read_exact(body).await;
    result?;
    Ok(Reply {
        header,
        body: Bytes::from(body.into_inner()),
    })
}
