//! One serialized blocking worker per script host; no detached retries or sinks.
use crate::{Compiled, Error, Host, Outcome};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
};

#[derive(Clone, Debug)]
pub struct Config {
    /// Bounds queued work plus results not yet consumed/dropped by callers.
    pub capacity: usize,
    pub input_bytes: usize,
    pub source_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            capacity: 16,
            input_bytes: 16384,
            source_bytes: 65536,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Running { generation: u64 },
    Faulted { generation: u64, error: Error },
    Stopped { generation: u64 },
}
enum Request {
    Invoke {
        generation: u64,
        function: String,
        input: String,
        result: oneshot::Sender<Outcome>,
    },
    Reload {
        generation: u64,
        compiled: Box<Compiled>,
        result: oneshot::Sender<Result<u64, Error>>,
    },
    Wake,
}
impl Request {
    fn cancel(self) {
        match self {
            Self::Invoke {
                generation, result, ..
            } => {
                let _ = result.send(Outcome {
                    generation,
                    outputs: Vec::new(),
                    error: Some(Error::Stopped),
                });
            }
            Self::Reload { result, .. } => {
                let _ = result.send(Err(Error::Stopped));
            }
            Self::Wake => {}
        }
    }
}
struct Shared {
    shutdown_callback: Mutex<Option<String>>,
    shutdown_output: Mutex<Option<Outcome>>,
    config: Config,
    send: mpsc::Sender<Message>,
    slots: Arc<Semaphore>,
    gate: Mutex<()>,
    stopped: AtomicBool,
    status: watch::Sender<Status>,
}
#[derive(Clone)]
pub struct Handle(Arc<Shared>);
pub struct Invocation {
    result: oneshot::Receiver<Outcome>,
    _slot: Arc<OwnedSemaphorePermit>,
}
impl Invocation {
    /// Waiting confirms execution only, not delivery of its opaque outputs.
    pub async fn wait(self) -> Result<Outcome, Error> {
        self.result
            .await
            .map_err(|_| Error::Worker("invocation reply lost".into()))
    }
}
pub struct Reload {
    result: oneshot::Receiver<Result<u64, Error>>,
    _slot: Arc<OwnedSemaphorePermit>,
}
impl Reload {
    pub async fn wait(self) -> Result<u64, Error> {
        self.result
            .await
            .map_err(|_| Error::Worker("reload reply lost".into()))?
    }
}
impl Handle {
    pub fn set_shutdown_callback(&self, function: Option<String>) -> Result<(), Error> {
        if function
            .as_ref()
            .is_some_and(|function| function.is_empty() || function.len() > 256)
        {
            return Err(Error::InputExceeded);
        }
        let _gate = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.0.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        *self
            .0
            .shutdown_callback
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = function;
        Ok(())
    }
    pub fn status(&self) -> watch::Receiver<Status> {
        self.0.status.subscribe()
    }
    fn submit(&self, request: Request) -> Result<Arc<OwnedSemaphorePermit>, Error> {
        let _gate = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.0.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let permit = Arc::new(permit);
        self.0
            .send
            .try_send(Message {
                request,
                _slot: Some(permit.clone()),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Stopped,
            })?;
        Ok(permit)
    }
    pub fn invoke(
        &self,
        generation: u64,
        function: String,
        input: String,
    ) -> Result<Invocation, Error> {
        if function.is_empty() || function.len() > 256 || input.len() > self.0.config.input_bytes {
            return Err(Error::InputExceeded);
        }
        let (result, received) = oneshot::channel();
        let slot = self.submit(Request::Invoke {
            generation,
            function,
            input,
            result,
        })?;
        Ok(Invocation {
            result: received,
            _slot: slot,
        })
    }
    /// Compilation is already complete; queue order defines the replacement boundary.
    pub fn reload(&self, generation: u64, compiled: Compiled) -> Result<Reload, Error> {
        if compiled.limits.source_bytes > self.0.config.source_bytes {
            return Err(Error::InputExceeded);
        }
        let (result, received) = oneshot::channel();
        let slot = self.submit(Request::Reload {
            generation,
            compiled: Box::new(compiled),
            result,
        })?;
        Ok(Reload {
            result: received,
            _slot: slot,
        })
    }
    /// Signal shutdown without joining; the Worker owner must still await shutdown.
    pub fn request_stop(&self) {
        let _gate = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
        self.0.stopped.store(true, Ordering::Release);
        // Full means queued work already wakes blocking_recv. No stop request
        // waits for an output receipt or an available semaphore slot.
        let _ = self.0.send.try_send(Message {
            request: Request::Wake,
            _slot: None,
        });
    }
}
pub struct Worker {
    handle: Handle,
    task: Option<JoinHandle<()>>,
}
impl Worker {
    pub fn spawn(compiled: Compiled, config: Config) -> Result<Self, Error> {
        if config.capacity == 0
            || config.capacity > Semaphore::MAX_PERMITS
            || config.input_bytes == 0
            || config.source_bytes == 0
            || compiled.limits.source_bytes > config.source_bytes
        {
            return Err(Error::InvalidConfig);
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|error| Error::Worker(error.to_string()))?;
        let (send, receive) = mpsc::channel(config.capacity);
        let (status, _) = watch::channel(Status::Running { generation: 1 });
        let shared = Arc::new(Shared {
            shutdown_callback: Mutex::new(None),
            shutdown_output: Mutex::new(None),
            slots: Arc::new(Semaphore::new(config.capacity)),
            config,
            send,
            gate: Mutex::new(()),
            stopped: AtomicBool::new(false),
            status,
        });
        let owner = shared.clone();
        let task = runtime.spawn_blocking(move || run(Host::new(compiled), receive, owner));
        Ok(Self {
            handle: Handle(shared),
            task: Some(task),
        })
    }
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }
    /// Finish the current invocation, cancel unstarted work, and join the worker.
    pub async fn shutdown(self) -> Result<(), Error> {
        self.shutdown_with_output().await.map(|_| ())
    }
    /// The callback runs once in the worker after queued ordinary work is cancelled.
    /// Its opaque outputs remain owned by the caller and require retirement fencing.
    pub async fn shutdown_with_output(mut self) -> Result<Option<Outcome>, Error> {
        self.handle.request_stop();
        self.task
            .take()
            .expect("owned worker")
            .await
            .map_err(|error| Error::Worker(error.to_string()))?;
        Ok(self
            .handle
            .0
            .shutdown_output
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take())
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.handle.request_stop();
    }
}
struct Message {
    request: Request,
    _slot: Option<Arc<OwnedSemaphorePermit>>,
}
fn run(mut host: Host, mut receive: mpsc::Receiver<Message>, shared: Arc<Shared>) {
    while let Some(Message { request, _slot }) = receive.blocking_recv() {
        if shared.stopped.load(Ordering::Acquire) {
            request.cancel();
            receive.close();
            while let Ok(request) = receive.try_recv() {
                request.request.cancel();
            }
            break;
        }
        match request {
            Request::Invoke {
                generation,
                function,
                input,
                result,
            } => {
                let outcome = host.invoke(generation, &function, &input);
                if let Some(Error::Execution(reason)) = &outcome.error {
                    shared.status.send_replace(Status::Faulted {
                        generation: host.generation(),
                        error: Error::Execution(reason.clone()),
                    });
                }
                let _ = result.send(outcome);
            }
            Request::Reload {
                generation,
                compiled,
                result,
            } => {
                let outcome = if generation != host.generation() {
                    Err(Error::Stale)
                } else {
                    host.reload(*compiled)
                };
                if let Ok(generation) = outcome {
                    shared.status.send_replace(Status::Running { generation });
                }
                let _ = result.send(outcome);
            }
            Request::Wake => {}
        }
    }
    if let Some(function) = shared
        .shutdown_callback
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        let output = host.invoke(host.generation(), &function, "");
        *shared
            .shutdown_output
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(output);
    }
    shared.stopped.store(true, Ordering::Release);
    shared.status.send_replace(Status::Stopped {
        generation: host.generation(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Limits, Output};
    use std::{
        sync::{Condvar, atomic::AtomicUsize},
        time::Duration,
    };
    use tokio::{sync::Notify, time::timeout};

    #[tokio::test]
    async fn stop_finishes_current_invocation_cancels_queue_and_reload_then_joins() {
        let mut compiled = Compiled::new(
            "fn input(v){gate();send(v);} fn bye(v){publish(\"bye\");}",
            Limits::default(),
            true,
        )
        .unwrap();
        let replacement = Compiled::new("fn input(v){send(v);}", Limits::default(), true).unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let count = Arc::new(AtomicUsize::new(0));
        let (signal, gate, calls) = (entered.clone(), release.clone(), count.clone());
        compiled.engine.register_fn("gate", move || {
            calls.fetch_add(1, Ordering::SeqCst);
            signal.notify_one();
            let guard = gate.0.lock().unwrap();
            let (guard, _) = gate
                .1
                .wait_timeout_while(guard, Duration::from_secs(5), |released| !*released)
                .unwrap();
            assert!(*guard, "test gate release");
        });
        let worker = Worker::spawn(
            compiled,
            Config {
                capacity: 3,
                ..Config::default()
            },
        )
        .unwrap();
        let handle = worker.handle();
        let current = handle.invoke(1, "input".into(), "current".into()).unwrap();
        timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        let queued = handle.invoke(1, "input".into(), "queued".into()).unwrap();
        let reload = handle.reload(1, replacement).unwrap();
        handle.set_shutdown_callback(Some("bye".into())).unwrap();
        handle.request_stop();
        assert!(matches!(
            handle.invoke(1, "input".into(), "late".into()),
            Err(Error::Stopped)
        ));
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        let stopped = timeout(Duration::from_secs(3), worker.shutdown_with_output())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(stopped.outputs, vec![Output::Publish("bye".into())]);
        assert_eq!(stopped.error, None);
        assert_eq!(
            current.wait().await.unwrap().outputs,
            vec![Output::Send("current".into())]
        );
        assert_eq!(queued.wait().await.unwrap().error, Some(Error::Stopped));
        assert_eq!(reload.wait().await, Err(Error::Stopped));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(*handle.status().borrow(), Status::Stopped { generation: 1 });
    }

    #[tokio::test]
    async fn native_panic_runs_off_async_thread_and_fault_does_not_restart_worker() {
        let async_thread = std::thread::current().id();
        let observed = Arc::new(Mutex::new(None));
        let thread = observed.clone();
        let mut compiled =
            Compiled::new("fn input(v){publish(v);crash();}", Limits::default(), true).unwrap();
        compiled
            .engine
            .register_fn("crash", move || -> Result<(), Box<rhai::EvalAltResult>> {
                *thread.lock().unwrap() = Some(std::thread::current().id());
                panic!("injected worker native panic")
            });
        let worker = Worker::spawn(compiled, Config::default()).unwrap();
        let handle = worker.handle();
        handle.set_shutdown_callback(Some("input".into())).unwrap();
        let unrelated = Worker::spawn(
            Compiled::new("fn input(v){publish(v);}", Limits::default(), true).unwrap(),
            Config::default(),
        )
        .unwrap();
        let result = handle
            .invoke(1, "input".into(), "prefix".into())
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_ne!(observed.lock().unwrap().unwrap(), async_thread);
        assert_eq!(result.outputs, vec![Output::Publish("prefix".into())]);
        assert_eq!(result.error, Some(Error::Execution("native panic".into())));
        assert!(matches!(
            *handle.status().borrow(),
            Status::Faulted { generation: 1, .. }
        ));
        assert_eq!(
            handle
                .invoke(1, "input".into(), "retry".into())
                .unwrap()
                .wait()
                .await
                .unwrap()
                .error,
            Some(Error::Faulted)
        );
        let progressed = timeout(
            Duration::from_secs(3),
            unrelated
                .handle()
                .invoke(1, "input".into(), "unrelated".into())
                .unwrap()
                .wait(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(progressed.error, None);
        assert_eq!(
            progressed.outputs,
            vec![Output::Publish("unrelated".into())]
        );
        unrelated.shutdown().await.unwrap();
        let stopped = worker.shutdown_with_output().await.unwrap().unwrap();
        assert_eq!(stopped.error, Some(Error::Faulted));
        assert!(stopped.outputs.is_empty());
    }
}
