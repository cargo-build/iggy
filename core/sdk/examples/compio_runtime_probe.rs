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

//! Feasibility probe for local compio I/O behind Send completion handles.
//! This exercises the runtime boundary, not the Iggy protocol or SDK.

#[cfg(target_os = "linux")]
mod probe {
    use compio::BufResult;
    use compio::io::{AsyncReadExt, AsyncWriteExt};
    use compio::net::{TcpListener, TcpStream};
    use compio::runtime::Runtime;
    use std::error::Error;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;

    struct CancellationWitness(Arc<AtomicBool>);

    impl Drop for CancellationWitness {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    pub fn run() -> Result<(), Box<dyn Error>> {
        require_no_tokio()?;
        let runtime = Runtime::new()?;
        if !runtime.driver_type().is_iouring() {
            return Err(io::Error::other("probe requires the io_uring driver").into());
        }
        runtime.block_on(async {
            require_no_tokio()?;
            timer_completion().await?;
            timer_cancellation().await?;
            tcp_roundtrip().await?;
            require_no_tokio()?;
            Ok::<(), Box<dyn Error>>(())
        })?;
        println!(
            "io_uring: TCP round trip, Send task handles, timer completion/cancellation; no Tokio runtime"
        );
        Ok(())
    }

    async fn timer_completion() -> Result<(), Box<dyn Error>> {
        let timer = require_send(compio::runtime::spawn(async {
            compio::time::sleep(Duration::from_millis(1)).await;
        }));
        timer.await?;
        Ok(())
    }

    async fn timer_cancellation() -> Result<(), Box<dyn Error>> {
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = CancellationWitness(Arc::clone(&dropped));
        let (started, ready) = oneshot::channel();
        let timer = require_send(compio::runtime::spawn(async move {
            let _witness = witness;
            let _ = started.send(());
            compio::time::sleep(Duration::from_secs(3600)).await;
        }));
        ready.await?;
        drop(timer);
        compio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::Acquire) {
                compio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }

    async fn tcp_roundtrip() -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = require_send(compio::runtime::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let BufResult(result, bytes) = socket.read_exact(vec![0; 4]).await;
            result?;
            let BufResult(result, _) = socket.write_all(bytes).await;
            result?;
            Ok::<(), io::Error>(())
        }));
        let client = require_send(compio::runtime::spawn(async move {
            let mut socket = TcpStream::connect(address).await?;
            let BufResult(result, _) = socket.write_all(b"iggy".to_vec()).await;
            result?;
            let BufResult(result, bytes) = socket.read_exact(vec![0; 4]).await;
            result?;
            Ok::<Vec<u8>, io::Error>(bytes)
        }));
        let received = client.await??;
        server.await??;
        if received != b"iggy" {
            return Err(io::Error::other("TCP reply differs from the sent bytes").into());
        }
        Ok(())
    }

    fn require_send<T: Send>(value: T) -> T {
        value
    }

    fn require_no_tokio() -> io::Result<()> {
        match tokio::runtime::Handle::try_current() {
            Ok(_) => Err(io::Error::other("probe must run outside a Tokio runtime")),
            Err(_) => Ok(()),
        }
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    probe::run()
}

#[cfg(not(target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err(std::io::Error::other("this io_uring runtime probe requires Linux").into())
}
