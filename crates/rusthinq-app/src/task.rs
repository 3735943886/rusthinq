//! A spawned task stays owned even when its enclosing service future is dropped.
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

pub(crate) struct OwnedTask<T>(JoinHandle<T>);
impl<T: Send + 'static> OwnedTask<T> {
    pub(crate) fn spawn(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self(tokio::spawn(future))
    }
}
impl<T> Future for OwnedTask<T> {
    type Output = Result<T, JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
impl<T> Drop for OwnedTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Finite filesystem work must finish before its owner releases the storage scope.
/// Unlike an async task, started blocking work cannot be aborted. Dropping this
/// owner waits synchronously; closures must never depend on async runtime progress.
pub(crate) struct StorageTask<T> {
    task: JoinHandle<()>,
    finished: std::sync::mpsc::Receiver<()>,
    result: tokio::sync::oneshot::Receiver<T>,
}
impl<T: Send + 'static> StorageTask<T> {
    pub(crate) fn spawn(work: impl FnOnce() -> T + Send + 'static) -> Self {
        let (finished, receiver) = std::sync::mpsc::channel();
        let (result, received) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            // Closure ownership releases this sender on success, panic, or cancellation
            // before starting. Disconnection also wakes the dropping owner.
            let _finished = finished;
            let _ = result.send(work());
        });
        Self {
            task,
            finished: receiver,
            result: received,
        }
    }
}
impl<T> Future for StorageTask<T> {
    type Output = std::io::Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match std::task::ready!(Pin::new(&mut self.task).poll(cx)) {
            Ok(()) => Poll::Ready(self.result.try_recv().map_err(std::io::Error::other)),
            Err(error) => Poll::Ready(Err(std::io::Error::other(error))),
        }
    }
}
impl<T> Drop for StorageTask<T> {
    fn drop(&mut self) {
        let _ = self.finished.recv();
    }
}

/// Normal shutdown signals then joins; cancellation also signals dependent actors.
pub(crate) struct Shutdown(watch::Sender<bool>);
impl Shutdown {
    pub(crate) fn new(stopped: bool) -> (Self, watch::Receiver<bool>) {
        let (sender, receiver) = watch::channel(stopped);
        (Self(sender), receiver)
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.0.subscribe()
    }
    pub(crate) fn stop(&self) {
        self.0.send_replace(true);
    }
}
impl Drop for Shutdown {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn dropping_storage_owner_waits_for_started_write_and_releases_result() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (released, dropped) = std::sync::mpsc::channel();
        struct Storage(std::sync::mpsc::Sender<()>);
        impl Drop for Storage {
            fn drop(&mut self) {
                self.0.send(()).unwrap();
            }
        }
        let task = StorageTask::spawn(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            Storage(released)
        });
        ready.await.unwrap();
        let releaser = std::thread::spawn(move || release.send(()).unwrap());
        drop(task);
        dropped
            .try_recv()
            .expect("storage result released before owner drop returns");
        releaser.join().unwrap();
    }

    #[tokio::test]
    async fn storage_task_preserves_results_and_panics_without_hanging_drop() {
        assert_eq!(StorageTask::spawn(|| 42).await.unwrap(), 42);
        assert!(
            StorageTask::spawn(|| panic!("storage panic"))
                .await
                .unwrap_err()
                .get_ref()
                .unwrap()
                .downcast_ref::<JoinError>()
                .unwrap()
                .is_panic()
        );
    }

    #[tokio::test]
    async fn dropping_owner_cancels_started_task_instead_of_detaching_it() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (released, dropped) = tokio::sync::oneshot::channel();
        let task = OwnedTask::spawn(async move {
            struct Release(Option<tokio::sync::oneshot::Sender<()>>);
            impl Drop for Release {
                fn drop(&mut self) {
                    let _ = self.0.take().unwrap().send(());
                }
            }
            let _release = Release(Some(released));
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        drop(task);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn joined_task_preserves_result_and_shutdown_drop_notifies_receivers() {
        assert_eq!(OwnedTask::spawn(async { 42 }).await.unwrap(), 42);
        let (shutdown, mut stopped) = Shutdown::new(false);
        let subscribed = shutdown.subscribe();
        drop(shutdown);
        stopped.changed().await.unwrap();
        assert!(*stopped.borrow());
        assert!(*subscribed.borrow());
    }
}
