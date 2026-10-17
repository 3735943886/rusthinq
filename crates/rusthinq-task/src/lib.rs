//! Blocking work stays owned by a draining supervisor when its receipt is cancelled.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    sync::{Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
};

type Job = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
type Finished = Option<Result<(), String>>;
#[derive(Default)]
struct Admission {
    sender: Option<mpsc::UnboundedSender<Job>>,
    started: bool,
    closed: bool,
}
struct Shared {
    admission: Mutex<Admission>,
    slots: Arc<Semaphore>,
    finished: watch::Sender<Finished>,
}

/// A bounded group with nonblocking drop and repeatable asynchronous shutdown.
/// Dropping the last handle closes admission; its supervisor drains accepted jobs.
/// The Tokio runtime must remain running until shutdown completes.
#[derive(Clone)]
pub struct Supervisor(Arc<Shared>);
impl Supervisor {
    /// Does not require a running Tokio runtime until the first job is submitted.
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Shared {
            admission: Mutex::new(Admission::default()),
            slots: Arc::new(Semaphore::new(capacity)),
            finished: watch::channel(None).0,
        }))
    }
    pub fn available(&self) -> usize {
        self.0.slots.available_permits()
    }
    pub fn try_spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> io::Result<Completion<T>> {
        let mut admission = self.0.admission.lock().unwrap_or_else(|e| e.into_inner());
        if admission.closed {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "task supervisor closed",
            ));
        }
        let slot = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "task capacity exceeded"))?;
        let runtime = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        let sender = admission.sender.get_or_insert_with(|| {
            let (sender, receiver) = mpsc::unbounded_channel();
            runtime.spawn(supervise(receiver, self.0.finished.clone()));
            sender
        });
        let (result, received) = oneshot::channel();
        // The supervisor owns both the join and its capacity lease. Cancelling the
        // receipt cannot abort the join or admit a replacement before completion.
        sender
            .send(Box::pin(async move {
                let outcome = tokio::task::spawn_blocking(work).await;
                let failure = outcome.as_ref().err().map(ToString::to_string);
                drop(slot);
                let _ = result.send(outcome.map_err(io::Error::other));
                failure.map_or(Ok(()), |error| Err(io::Error::other(error)))
            }))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "task supervisor stopped"))?;
        admission.started = true;
        Ok(Completion(received))
    }
    pub fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Completion<T> {
        self.try_spawn_blocking(work).unwrap_or_else(|error| {
            let (sender, receiver) = oneshot::channel();
            let _ = sender.send(Err(error));
            Completion(receiver)
        })
    }
    /// Close admission and await all accepted jobs, including cancelled receipts.
    /// Cancelling this wait does not cancel draining; another handle can wait again.
    /// Stop/drop long-lived workers before calling this method.
    pub async fn shutdown(&self) -> io::Result<()> {
        let mut finished = self.0.finished.subscribe();
        {
            let mut admission = self.0.admission.lock().unwrap_or_else(|e| e.into_inner());
            admission.closed = true;
            admission.sender.take();
            if !admission.started {
                self.0.finished.send_replace(Some(Ok(())));
            }
        }
        finished
            .wait_for(|value| value.is_some())
            .await
            .map_err(io::Error::other)?;
        match finished.borrow().as_ref() {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => Err(io::Error::other(error.clone())),
            None => Err(io::Error::other("task supervisor completion missing")),
        }
    }
}

pub struct Completion<T>(oneshot::Receiver<io::Result<T>>);
impl<T> Future for Completion<T> {
    type Output = io::Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.map_err(io::Error::other).and_then(|result| result))
    }
}
async fn supervise(mut incoming: mpsc::UnboundedReceiver<Job>, finished: watch::Sender<Finished>) {
    let mut tasks = JoinSet::new();
    let mut accepting = true;
    let mut failure = None;
    while accepting || !tasks.is_empty() {
        tokio::select! {
            job = incoming.recv(), if accepting => match job {
                Some(job) => { tasks.spawn(job); },
                None => accepting = false,
            },
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                let result = joined.map_err(io::Error::other).and_then(|result| result);
                if let Err(error) = result { failure.get_or_insert_with(|| error.to_string()); }
            },
        }
    }
    finished.send_replace(Some(failure.map_or(Ok(()), Err)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn cancelled_receipt_and_shutdown_keep_capacity_until_work_finishes() {
        let tasks = Supervisor::new(1);
        let (started, ready) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tasks
            .try_spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv_timeout(Duration::from_secs(5)).unwrap();
                42
            })
            .unwrap();
        ready.await.unwrap();
        drop(task);
        assert_eq!(tasks.available(), 0);
        assert!(
            matches!(tasks.try_spawn_blocking(|| ()), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
        );
        let mut shutdown = Box::pin(tasks.shutdown());
        std::future::poll_fn(|cx| {
            assert!(shutdown.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(shutdown);
        // Neither cancelled future can block this single-threaded executor.
        tokio::task::yield_now().await;
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), tasks.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tasks.available(), 1);
        assert!(
            matches!(tasks.try_spawn_blocking(|| ()), Err(e) if e.kind() == io::ErrorKind::NotConnected)
        );
        tasks.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn last_scope_drop_still_drains_work_and_discarded_results() {
        // Use a sender's lifetime as the resource-release signal.
        let (released, mut dropped) = oneshot::channel::<()>();
        let tasks = Supervisor::new(1);
        let (started, ready) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = tasks.spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(5)).unwrap();
            released
        });
        ready.await.unwrap();
        drop(task);
        drop(tasks);
        assert!(matches!(
            dropped.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        tokio::task::yield_now().await;
        release.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), dropped)
                .await
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn panics_are_reported_even_if_the_receipt_was_cancelled() {
        let tasks = Supervisor::new(1);
        let task = tasks.spawn_blocking(|| panic!("blocking work panic"));
        drop(task);
        let error = tasks.shutdown().await.unwrap_err();
        assert!(error.to_string().contains("blocking work panic"));
    }

    #[tokio::test]
    async fn normal_results_and_unused_groups_shutdown() {
        let tasks = Supervisor::new(1);
        assert_eq!(tasks.spawn_blocking(|| 42).await.unwrap(), 42);
        tasks.shutdown().await.unwrap();
        Supervisor::new(1).shutdown().await.unwrap();
    }
}
