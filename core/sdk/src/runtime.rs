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

//! Per-client scheduling and time effects on the caller's existing runtime.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(all(feature = "io-uring", target_os = "linux"))]
use compio::net::ToSocketAddrsAsync;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use futures::FutureExt;
use futures::TryFutureExt;
use futures::future::{AbortHandle as FutureAbortHandle, Abortable};
use tokio::sync::oneshot;
use tokio::time::Instant;

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static TASK_ID: u64;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum Runtime {
    #[default]
    Tokio,
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    Compio,
}

impl Runtime {
    pub(crate) fn sleep_until(
        self,
        deadline: Instant,
    ) -> impl Future<Output = ()> + Send + 'static {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        {
            match self {
                Self::Tokio => futures::future::Either::Left(tokio::time::sleep_until(deadline)),
                Self::Compio => {
                    futures::future::Either::Right(compio_sleep_until(deadline.into_std()))
                }
            }
        }
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        {
            let Self::Tokio = self;
            tokio::time::sleep_until(deadline)
        }
    }

    pub(crate) async fn sleep(self, duration: Duration) {
        match self {
            Self::Tokio => tokio::time::sleep(duration).await,
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            Self::Compio => compio_sleep_until(std::time::Instant::now() + duration).await,
        }
    }

    pub(crate) fn timeout_at<F: Future>(
        self,
        deadline: Instant,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Elapsed>> {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        {
            match self {
                Self::Tokio => futures::future::Either::Left(
                    tokio::time::timeout_at(deadline, future).map_err(|_| Elapsed),
                ),
                Self::Compio => futures::future::Either::Right(compio_timeout_at(deadline, future)),
            }
        }
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        {
            let Self::Tokio = self;
            tokio::time::timeout_at(deadline, future).map_err(|_| Elapsed)
        }
    }

    pub(crate) fn timeout<F: Future>(
        self,
        duration: Duration,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Elapsed>> {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        {
            match self {
                Self::Tokio => futures::future::Either::Left(
                    tokio::time::timeout(duration, future).map_err(|_| Elapsed),
                ),
                Self::Compio => futures::future::Either::Right(
                    match std::time::Instant::now().checked_add(duration) {
                        Some(deadline) => futures::future::Either::Left(compio_timeout_at(
                            Instant::from_std(deadline),
                            future,
                        )),
                        None => futures::future::Either::Right(future.map(Ok)),
                    },
                ),
            }
        }
        #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
        {
            let Self::Tokio = self;
            tokio::time::timeout(duration, future).map_err(|_| Elapsed)
        }
    }

    pub(crate) fn spawn<F: Future<Output = ()> + Send + 'static>(self, future: F) -> Task {
        let (task, run) = Task::prepare(future);
        match self {
            Self::Tokio => drop(tokio::spawn(run)),
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            Self::Compio => compio::runtime::spawn(run).detach(),
        }
        task
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub(crate) fn spawn_compio_local<F: Future<Output = ()> + 'static>(future: F) -> Task {
        let (task, run) = Task::prepare(future);
        compio::runtime::spawn(run).detach();
        task
    }

    pub(crate) async fn lookup_host(self, address: &str) -> io::Result<Vec<SocketAddr>> {
        match self {
            Self::Tokio => tokio::net::lookup_host(address)
                .await
                .map(|addresses| addresses.collect()),
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            Self::Compio => {
                let address = address.to_owned();
                compio::runtime::spawn(async move {
                    address
                        .to_socket_addrs_async()
                        .await
                        .map(|addresses| addresses.collect())
                })
                .await
                .map_err(io::Error::from)?
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Elapsed;

impl Display for Elapsed {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("deadline has elapsed")
    }
}

impl Error for Elapsed {}

/// Dropping this observation handle detaches; cancellation remains explicit.
#[derive(Debug)]
pub(crate) struct Task {
    completion: oneshot::Receiver<Result<(), TaskError>>,
    abort: AbortHandle,
}

impl Task {
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    pub(crate) fn abort_handle(&self) -> AbortHandle {
        self.abort.clone()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }

    fn prepare<F: Future<Output = ()> + 'static>(
        future: F,
    ) -> (Self, impl Future<Output = ()> + 'static) {
        let id = next_task_id();
        let state = Arc::new(TaskState {
            id,
            finished: AtomicBool::new(false),
        });
        let (abort, registration) = FutureAbortHandle::new_pair();
        let (sender, completion) = oneshot::channel();
        // Capture the guard before spawning so even an unpolled task marks completion.
        let guard = Completion {
            state: Arc::clone(&state),
            sender: Some(sender),
        };
        let run = TASK_ID.scope(id, async move {
            let result = Abortable::new(future, registration)
                .await
                .map_err(|_| TaskError::Cancelled);
            guard.finish(result);
        });
        let task = Self {
            completion,
            abort: AbortHandle { abort, state },
        };
        (task, run)
    }
}

impl Future for Task {
    type Output = Result<(), TaskError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().completion)
            .poll(context)
            .map(|result| match result {
                Ok(result) => result,
                Err(_) => Err(TaskError::Terminated),
            })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AbortHandle {
    abort: FutureAbortHandle,
    state: Arc<TaskState>,
}

impl AbortHandle {
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    pub(crate) fn id(&self) -> u64 {
        self.state.id
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.state.finished.load(Ordering::Acquire)
    }
}

pub(crate) fn task_id() -> Option<u64> {
    TASK_ID.try_with(|id| *id).ok()
}

fn next_task_id() -> u64 {
    let mut current = NEXT_TASK_ID.load(Ordering::Relaxed);
    loop {
        let next = current
            .checked_add(1)
            .expect("runtime task identity space exhausted");
        match NEXT_TASK_ID.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return current,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskError {
    Cancelled,
    /// The executor dropped the future or the future panicked before reporting completion.
    Terminated,
}

impl Display for TaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Cancelled => "task cancelled",
            Self::Terminated => "task terminated before reporting completion",
        })
    }
}

impl Error for TaskError {}

#[derive(Debug)]
struct TaskState {
    id: u64,
    finished: AtomicBool,
}

struct Completion {
    state: Arc<TaskState>,
    sender: Option<oneshot::Sender<Result<(), TaskError>>>,
}

impl Completion {
    fn finish(mut self, result: Result<(), TaskError>) {
        self.state.finished.store(true, Ordering::Release);
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.state.finished.store(true, Ordering::Release);
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn compio_timeout_at<F: Future>(
    deadline: Instant,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    // Nested operation futures must not be embedded twice in an async match state.
    let mut future = Box::pin(future);
    async move {
        let mut timer = std::pin::pin!(compio_sleep_until(deadline.into_std()));
        std::future::poll_fn(|context| {
            if let Poll::Ready(value) = future.as_mut().poll(context) {
                return Poll::Ready(Ok(value));
            }
            if deadline.into_std() <= std::time::Instant::now() {
                return Poll::Ready(Err(Elapsed));
            }
            timer.as_mut().poll(context).map(|()| Err(Elapsed))
        })
        .await
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn compio_sleep_until(deadline: std::time::Instant) -> impl Future<Output = ()> + Send + 'static {
    // The Send join handle keeps the local timer out of the caller's future.
    // Unlike a detached task, dropping this handle cancels the pending timer.
    let timer = compio::runtime::spawn(compio::time::sleep_until(deadline));
    async move {
        if let Err(error) = timer.await {
            error.resume_unwind();
            panic!("compio timer task was cancelled before its deadline");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::Instant;

    use super::{AbortHandle, Elapsed, Runtime, Task, TaskError, task_id};

    struct DropWitness(Arc<AtomicBool>);

    impl Drop for DropWitness {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn require_send<T: Send>(value: T) -> T {
        value
    }

    fn require_send_sync<T: Send + Sync>() {}

    #[tokio::test(start_paused = true)]
    async fn given_tokio_time_when_waiting_should_preserve_paused_clock_and_ready_precedence() {
        let runtime = Runtime::default();
        let wait = Duration::from_millis(25);
        let started = Instant::now();
        assert_eq!(
            require_send(runtime.timeout(wait, future::pending::<()>())).await,
            Err(Elapsed)
        );
        assert_eq!(started.elapsed(), wait);
        assert_eq!(
            runtime
                .timeout_at(started, future::ready("already ready"))
                .await,
            Ok("already ready")
        );
        assert_eq!(
            runtime.timeout(Duration::ZERO, future::ready(7)).await,
            Ok(7)
        );
        assert_eq!(
            runtime.timeout(Duration::MAX, future::ready(8)).await,
            Ok(8)
        );
        let started = Instant::now();
        require_send(runtime.sleep_until(started + wait)).await;
        require_send(runtime.sleep(wait)).await;
        assert_eq!(started.elapsed(), wait * 2);
        assert_eq!(
            require_send(runtime.lookup_host("127.0.0.1:1234"))
                .await
                .unwrap(),
            ["127.0.0.1:1234".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn given_spawned_tasks_when_polled_should_report_their_own_abort_handle_identity() {
        assert_eq!(task_id(), None);
        require_send_sync::<AbortHandle>();
        let (sender, receiver) = oneshot::channel();
        let task = require_send(Runtime::Tokio.spawn(async move {
            let before = task_id();
            tokio::task::yield_now().await;
            sender.send((before, task_id())).unwrap();
        }));
        let handle = task.abort_handle();
        let second = Runtime::Tokio.spawn(async {});
        assert_ne!(handle.id(), second.abort_handle().id());
        assert_eq!(
            receiver.await.unwrap(),
            (Some(handle.id()), Some(handle.id()))
        );
        assert_eq!(task.await, Ok(()));
        assert!(handle.is_finished());
        assert_eq!(second.await, Ok(()));
        assert_eq!(task_id(), None);
    }

    #[tokio::test]
    async fn given_a_detached_task_when_its_handle_aborts_should_drop_the_running_future() {
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = DropWitness(Arc::clone(&dropped));
        let (started, ready) = oneshot::channel();
        let task = Runtime::Tokio.spawn(async move {
            let _witness = witness;
            started.send(()).unwrap();
            future::pending::<()>().await;
        });
        let handle = task.abort_handle();
        ready.await.unwrap();
        drop(task);
        tokio::task::yield_now().await;
        assert!(!handle.is_finished());
        assert!(!dropped.load(Ordering::Acquire));

        handle.abort();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !handle.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(handle.is_finished());
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn given_an_unpolled_task_when_aborted_should_complete_and_drop_its_future() {
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = DropWitness(Arc::clone(&dropped));
        let task = Runtime::Tokio.spawn(async move {
            let _witness = witness;
            panic!("an aborted task must not be polled");
        });
        task.abort();
        let handle = task.abort_handle();
        assert_eq!(task.await, Err(TaskError::Cancelled));
        assert!(handle.is_finished());
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn given_a_panicking_task_when_joined_should_report_termination_and_completion() {
        let task = Runtime::Tokio.spawn(async { panic!("task panic witness") });
        let handle = task.abort_handle();
        assert_eq!(task.await, Err(TaskError::Terminated));
        assert!(handle.is_finished());
    }

    #[tokio::test]
    async fn given_an_unpolled_runner_when_dropped_should_mark_the_task_finished() {
        let (task, run) = Task::prepare(future::pending());
        let handle = task.abort_handle();
        assert!(!task.is_finished());
        drop(run);
        assert!(task.is_finished());
        assert!(handle.is_finished());
        assert_eq!(task.await, Err(TaskError::Terminated));
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    #[test]
    fn given_compio_without_tokio_when_using_local_tasks_should_keep_send_handles_and_native_time()
    {
        let runtime = compio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            assert!(tokio::runtime::Handle::try_current().is_err());
            compio::time::timeout(Duration::from_secs(1), async {
                let runtime = Runtime::Compio;
                let value = Rc::new(42);
                let (sender, receiver) = oneshot::channel();
                let local = require_send(Runtime::spawn_compio_local(async move {
                    compio::time::sleep(Duration::from_millis(1)).await;
                    sender.send((*value, task_id())).unwrap();
                }));
                let handle = local.abort_handle();
                assert_eq!(receiver.await.unwrap(), (42, Some(handle.id())));
                assert_eq!(local.await, Ok(()));
                assert!(handle.is_finished());

                let dropped = Arc::new(AtomicBool::new(false));
                let witness = DropWitness(Arc::clone(&dropped));
                let task = require_send(runtime.spawn(async move {
                    let _witness = witness;
                    future::pending::<()>().await;
                }));
                task.abort();
                assert_eq!(task.await, Err(TaskError::Cancelled));
                assert!(dropped.load(Ordering::Acquire));

                require_send(runtime.sleep_until(Instant::now() + Duration::from_millis(1))).await;
                assert_eq!(
                    require_send(
                        runtime.timeout(Duration::from_millis(1), future::pending::<()>())
                    )
                    .await,
                    Err(Elapsed)
                );
                assert_eq!(
                    require_send(runtime.timeout_at(Instant::now(), future::ready(7))).await,
                    Ok(7)
                );
                assert_eq!(
                    runtime
                        .timeout_at(Instant::now(), future::pending::<()>())
                        .await,
                    Err(Elapsed)
                );
                assert_eq!(
                    runtime.timeout(Duration::MAX, future::ready(8)).await,
                    Ok(8)
                );
                assert_eq!(
                    require_send(runtime.lookup_host("127.0.0.1:1234"))
                        .await
                        .unwrap(),
                    ["127.0.0.1:1234".parse().unwrap()]
                );
                assert_eq!(
                    runtime
                        .lookup_host("invalid-address")
                        .await
                        .unwrap_err()
                        .kind(),
                    std::io::ErrorKind::InvalidInput
                );
                assert!(tokio::runtime::Handle::try_current().is_err());
            })
            .await
            .unwrap();
        });
    }
}
