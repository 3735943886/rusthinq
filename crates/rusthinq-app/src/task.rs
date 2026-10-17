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
