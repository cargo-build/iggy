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

use super::CompioTcpClient;
use crate::links::fake_cluster::{Answer, FakeCluster, UNANSWERED_CODE, node};
use crate::runtime::Runtime;
use crate::tcp::tcp_client::TcpClient;
use bytes::Bytes;
use iggy_common::{BinaryTransport, Client, ClientState, ClusterClient, IggyError, SystemClient, TcpClientConfig, UserClient, with_binary_deadline};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

const BUDGET: Duration = Duration::from_secs(5);

// The scripted peer runs Tokio on its own thread; the client runs only compio.
struct Peer {
    address: String,
    cluster: Arc<FakeCluster>,
    connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Peer {
    fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let cluster = FakeCluster::new(0);
        *cluster.nodes.lock().unwrap() = vec![node(0, "127.0.0.1", address.port(), true)];
        let served = Arc::clone(&cluster);
        let connections = Arc::new(Mutex::new(Vec::new()));
        let handles = Arc::clone(&connections);
        let (stop, stopped) = oneshot::channel();
        let endpoint = address.to_string();
        let address = endpoint.clone();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                tokio::pin!(stopped);
                loop {
                    tokio::select! {
                        _ = &mut stopped => break,
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            let task = tokio::spawn(Arc::clone(&served).serve(endpoint.clone(), stream));
                            handles.lock().unwrap().push(task.abort_handle());
                        }
                    }
                }
            });
        });
        Self { address, cluster, connections, stop: Some(stop), thread: Some(thread) }
    }

    fn client(&self) -> CompioTcpClient {
        CompioTcpClient::from_connection_string(&format!("iggy://iggy:iggy@{}", self.address)).unwrap()
    }

    fn cut_connections(&self) {
        self.connections.lock().unwrap().drain(..).for_each(|task| task.abort());
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn native_runtime() -> compio::runtime::Runtime {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let runtime = compio::runtime::Runtime::new().unwrap();
    assert!(runtime.driver_type().is_iouring());
    runtime
}

#[test]
fn given_native_compio_when_connecting_should_share_sessions_recover_and_shutdown_without_tokio() {
    let peer = Peer::new();
    let runtime = native_runtime();
    runtime.block_on(async {
        compio::time::timeout(BUDGET, async {
            let client = peer.client();
            client.connect().await.unwrap();
            assert_eq!(client.get_state().await, ClientState::Authenticated);
            assert_eq!(client.get_cluster_metadata().await.unwrap().nodes.len(), 1);
            client.login_user("iggy", "iggy").await.unwrap();
            client.ping().await.unwrap();
            let payload = vec![17; 3 * crate::links::test_support::DUPLEX_CAPACITY];
            const RAW_CODE: u32 = 777_777;
            peer.cluster.script(&peer.address, RAW_CODE, [Answer::Body(payload.clone())]);
            let reply = client.send_raw_with_response(RAW_CODE, Bytes::from(payload.clone())).await.unwrap();
            assert_eq!(reply.as_ref(), payload);
            assert_eq!(peer.cluster.received(RAW_CODE).first().unwrap().body, payload);
            assert!(client.keepalive().await.unwrap());
            let session = client.driver.session().identity().unwrap();

            peer.cut_connections();
            loop {
                match client.keepalive().await {
                    Ok(true) => break,
                    Ok(false) | Err(IggyError::Disconnected | IggyError::NotConnected) => {
                        compio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(error) => panic!("unexpected reconnect error: {error}"),
                }
            }
            client.ping().await.unwrap();
            assert_eq!(client.driver.session().identity().unwrap().session, session.session);
            assert!(peer.cluster.count(iggy_binary_protocol::codes::BIND_SESSION_CODE) > 0);
            assert!(tokio::runtime::Handle::try_current().is_err());

            client.disconnect().await.unwrap();
            assert!(!client.keepalive().await.unwrap());
            client.connect().await.unwrap();
            client.shutdown().await.unwrap();
            client.shutdown().await.unwrap();
            assert_eq!(client.connect().await, Err(IggyError::ClientShutdown));
        }).await.unwrap();
    });
}

#[test]
fn given_a_missing_reply_when_deadline_expires_should_keep_the_compio_connection_usable() {
    let peer = Peer::new();
    native_runtime().block_on(async {
        compio::time::timeout(BUDGET, async {
            let client = peer.client();
            client.connect().await.unwrap();
            let result = with_binary_deadline(Instant::now() + Duration::from_millis(20),
                client.send_raw_with_response(UNANSWERED_CODE, Bytes::new())).await;
            assert_eq!(result, Err(IggyError::TransientNotCommitted));
            client.ping().await.unwrap();
            assert!(tokio::runtime::Handle::try_current().is_err());
            client.shutdown().await.unwrap();
        }).await.unwrap();
    });
}

#[test]
fn given_feature_enabled_when_constructing_clients_should_preserve_defaults_and_reject_tls_downgrade() {
    let default = TcpClient::default();
    assert!(matches!(default.driver.runtime(), Runtime::Tokio));
    let config = Arc::new(TcpClientConfig { tls_enabled: true, ..Default::default() });
    assert!(matches!(CompioTcpClient::create(config), Err(IggyError::FeatureUnavailable)));
    assert!(CompioTcpClient::from_connection_string("http://localhost:3000").is_err());
    assert!(tokio::runtime::Handle::try_current().is_err());
}
