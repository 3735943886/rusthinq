use rusthinq_scripting::{
    Compiled, Error, Limits, Output,
    worker::{Config, Status, Worker},
};

fn compiled(source: &str) -> Compiled {
    Compiled::new(source, Limits::default(), true).unwrap()
}

#[tokio::test]
async fn invocations_and_reload_are_serialized_and_late_generation_is_rejected() {
    let replacement = compiled("fn input(v) {send(\"new:\"+v);}");
    let worker = Worker::spawn(
        compiled("let n=0;fn input(v){n+=1;send(n.to_string()+v);}"),
        Config::default(),
    )
    .unwrap();
    let handle = worker.handle();
    let first = handle.invoke(1, "input".into(), "a".into()).unwrap();
    let second = handle.invoke(1, "input".into(), "b".into()).unwrap();
    let reload = handle.reload(1, replacement).unwrap();
    let stale = handle.invoke(1, "input".into(), "late".into()).unwrap();
    assert_eq!(
        first.wait().await.unwrap().outputs,
        vec![Output::Send("1a".into())]
    );
    assert_eq!(
        second.wait().await.unwrap().outputs,
        vec![Output::Send("2b".into())]
    );
    assert_eq!(reload.wait().await.unwrap(), 2);
    assert_eq!(stale.wait().await.unwrap().error, Some(Error::Stale));
    assert_eq!(*handle.status().borrow(), Status::Running { generation: 2 });
    assert_eq!(
        handle
            .invoke(2, "input".into(), "fresh".into())
            .unwrap()
            .wait()
            .await
            .unwrap()
            .outputs,
        vec![Output::Send("new:fresh".into())]
    );
    assert!(Compiled::new("fn broken(", Limits::default(), true).is_err());
    assert_eq!(
        handle
            .reload(1, compiled("fn input(v) {}"))
            .unwrap()
            .wait()
            .await,
        Err(Error::Stale)
    );
    worker.shutdown().await.unwrap();
    assert_eq!(*handle.status().borrow(), Status::Stopped { generation: 2 });
    assert!(matches!(
        handle.invoke(2, "input".into(), "x".into()),
        Err(Error::Stopped)
    ));
}

#[tokio::test]
async fn outstanding_result_bounds_admission_and_shutdown_does_not_wait_for_receipt() {
    let worker = Worker::spawn(
        compiled("fn input(v){publish(v);}"),
        Config {
            capacity: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let handle = worker.handle();
    let receipt = handle.invoke(1, "input".into(), "held".into()).unwrap();
    assert!(matches!(
        handle.invoke(1, "input".into(), "overload".into()),
        Err(Error::Busy)
    ));
    assert!(matches!(
        handle.reload(1, compiled("fn input(v) {}")),
        Err(Error::Busy)
    ));
    // Shutdown is independent of the outstanding output/receipt capacity.
    worker.shutdown().await.unwrap();
    let result = receipt.wait().await.unwrap();
    assert!(result.error.is_none() || result.error == Some(Error::Stopped));
    assert_eq!(*handle.status().borrow(), Status::Stopped { generation: 1 });
}

#[tokio::test]
async fn faulted_worker_preserves_prefix_and_other_worker_progress_until_explicit_reload() {
    let worker = Worker::spawn(
        compiled("fn input(v){publish(v);throw \"broken\";}"),
        Config::default(),
    )
    .unwrap();
    let other = Worker::spawn(compiled("fn input(v){send(v);}"), Config::default()).unwrap();
    let handle = worker.handle();
    let result = handle
        .invoke(1, "input".into(), "prefix".into())
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(result.outputs, vec![Output::Publish("prefix".into())]);
    assert!(matches!(result.error, Some(Error::Execution(_))));
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
    assert_eq!(
        other
            .handle()
            .invoke(1, "input".into(), "healthy".into())
            .unwrap()
            .wait()
            .await
            .unwrap()
            .error,
        None
    );
    assert_eq!(
        handle
            .reload(1, compiled("fn input(v){send(v);}"))
            .unwrap()
            .wait()
            .await
            .unwrap(),
        2
    );
    assert_eq!(*handle.status().borrow(), Status::Running { generation: 2 });
    worker.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

#[tokio::test]
async fn admission_bounds_reject_before_faulting_script() {
    let worker = Worker::spawn(
        compiled("fn input(v){send(v);}"),
        Config {
            input_bytes: 4,
            ..Config::default()
        },
    )
    .unwrap();
    let handle = worker.handle();
    assert!(matches!(
        handle.invoke(1, "input".into(), "12345".into()),
        Err(Error::InputExceeded)
    ));
    assert!(matches!(
        handle.invoke(1, "x".repeat(257), "ok".into()),
        Err(Error::InputExceeded)
    ));
    assert_eq!(
        handle
            .invoke(1, "input".into(), "ok".into())
            .unwrap()
            .wait()
            .await
            .unwrap()
            .error,
        None
    );
    worker.shutdown().await.unwrap();
}
