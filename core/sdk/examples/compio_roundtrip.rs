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

//! One native compio publish/poll round trip against a matching local broker.

#[cfg(target_os = "linux")]
mod roundtrip {
    use iggy::io_uring::CompioTcpClient;
    use iggy::prelude::*;
    use std::error::Error;
    use std::io;
    use std::str::FromStr;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const PAYLOAD: &str = "compio io_uring round trip";

    pub fn run() -> Result<(), Box<dyn Error>> {
        let connection = std::env::args().nth(1).ok_or_else(|| {
            io::Error::other("usage: compio_roundtrip <iggy://user:password@host:port>")
        })?;
        let runtime = compio::runtime::Runtime::new()?;
        if !runtime.driver_type().is_iouring() {
            return Err(io::Error::other("this example requires the io_uring driver").into());
        }
        runtime.block_on(async {
            compio::time::timeout(Duration::from_secs(30), async {
                if tokio::runtime::Handle::try_current().is_ok() {
                    return Err(io::Error::other("run this example outside a Tokio runtime").into());
                }
                let client = CompioTcpClient::from_connection_string(&connection)?;
                client.connect().await?;
                let result = exchange(&client).await;
                let shutdown = client.shutdown().await;
                if let Err(error) = &shutdown {
                    eprintln!("Client shutdown failed: {error}");
                }
                result?;
                shutdown?;
                println!("Iggy native io_uring: authenticated, created resources, published/polled matching bytes and removed resources; no Tokio runtime");
                Ok::<(), Box<dyn Error>>(())
            }).await?
        })
    }

    async fn exchange(client: &CompioTcpClient) -> Result<(), Box<dyn Error>> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let name = format!("compio-{}-{stamp}", std::process::id());
        let stream = client
            .create_stream(&name, &StreamCreateOptions::default())
            .await?;
        let stream_id = Identifier::numeric(stream.id)?;
        let result = async {
            let topic = client
                .create_topic(
                    &stream_id,
                    "messages",
                    &TopicCreateOptions {
                        partitions_count: Some(1),
                        ..Default::default()
                    },
                )
                .await?;
            let topic_id = Identifier::numeric(topic.id)?;
            let mut messages = [IggyMessage::from_str(PAYLOAD)?];
            let response = client
                .send_messages(
                    &stream_id,
                    &topic_id,
                    &Partitioning::partition_id(0),
                    &mut messages,
                )
                .await?;
            let receipt = response
                .confirmations
                .first()
                .ok_or_else(|| io::Error::other("send returned no commit confirmation"))?;
            if receipt.stream_id != stream.id
                || receipt.topic_id != topic.id
                || receipt.partition_id != 0
            {
                return Err(io::Error::other("send confirmation names another partition").into());
            }
            let polled = client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    Some(0),
                    &Consumer::default(),
                    &PollingStrategy::offset(receipt.base_offset),
                    1,
                    false,
                )
                .await?;
            let message = polled.messages.first().ok_or_else(|| {
                io::Error::other("poll returned no message at the committed offset")
            })?;
            if polled.partition_id != 0 || message.payload.as_ref() != PAYLOAD.as_bytes() {
                return Err(io::Error::other(
                    "polled partition or bytes differ from the sent message",
                )
                .into());
            }
            Ok::<(), Box<dyn Error>>(())
        }
        .await;
        let cleanup = client.delete_stream(&stream_id).await;
        if let Err(error) = &cleanup {
            eprintln!("Could not remove probe stream {}: {error}", stream.id);
        }
        result?;
        cleanup?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    roundtrip::run()
}

#[cfg(not(target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err(std::io::Error::other("the io_uring client example requires Linux").into())
}
