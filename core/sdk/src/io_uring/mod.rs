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

//! Optional plain-TCP client on the caller's compio runtime.
//!
//! Enable `io-uring` on Linux and run all client operations inside an existing
//! compio runtime. No Tokio runtime is started. The existing default clients
//! and high-level producer/consumer helpers retain their Tokio requirement.
//! Operations require a live compio runtime and can panic outside it. Shut the
//! client down before dropping that runtime; constructors perform no I/O.

use crate::binary::impl_binary_client;
use crate::driver::OperationDriver;
use crate::links::{Dialer, ReconnectPolicy, ReplicaLinks};
use crate::runtime::Runtime;
use crate::session_owner::SessionOwner;
use crate::tcp::tcp_client::config_from_connection_string;
use iggy_common::{IggyError, TcpClientConfig};
use std::sync::Arc;

/// Shares the standard binary session and routing machinery with native compio I/O.
#[derive(Debug)]
pub struct CompioTcpClient {
    config: Arc<TcpClientConfig>,
    driver: OperationDriver,
}

impl_binary_client!(CompioTcpClient);

impl CompioTcpClient {
    /// Creates a plain-TCP client without opening sockets or starting a runtime.
    /// TLS configurations return `FeatureUnavailable` rather than downgrading.
    pub fn create(config: Arc<TcpClientConfig>) -> Result<Self, IggyError> {
        if config.tls_enabled {
            return Err(IggyError::FeatureUnavailable);
        }
        let session = Arc::new(SessionOwner::with_runtime(
            config.auto_login.clone(),
            config.pending_operations,
            Runtime::Compio,
        ));
        let links = ReplicaLinks::new(
            Dialer::CompioTcp(Arc::clone(&config)),
            config.server_address.clone(),
            ReconnectPolicy::from(&config.reconnection),
            session,
        );
        Ok(Self {
            config,
            driver: OperationDriver::new(links),
        })
    }

    /// Uses the same connection-string validation as the default TCP client.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        Self::create(Arc::new(config_from_connection_string(connection_string)?))
    }

    /// Pings bound replica links and restarts stopped reconnect attempts.
    pub async fn keepalive(&self) -> Result<bool, IggyError> {
        self.driver.links().keepalive().await
    }
}

#[cfg(test)]
mod tests;
