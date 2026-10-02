#![cfg(feature = "scripting")]
use rusthinq_app::scripts::Owner;
use rusthinq_lifecycle::{Device, Entry, Removal, SessionKey, Step};
use rusthinq_scripting::{Compiled, Error, Limits, Output, worker::Config};

fn device(incarnation: u64, generation: u64) -> Device {
    Device {
        entry: Entry {
            id: "a".into(),
            incarnation,
            last_generation: generation,
        },
        session: Some(SessionKey {
            incarnation,
            generation,
        }),
        online: true,
        offline_deadline: None,
        removal: None,
    }
}
fn script() -> Compiled {
    Compiled::new("fn input(v){publish(v);send(v);}", Limits::default(), true).unwrap()
}
#[tokio::test]
async fn completed_outputs_are_fenced_across_reconnect_and_reincarnation() {
    let mut owner = Owner::new(1).unwrap();
    let first = device(1, 10);
    owner
        .attach(
            std::slice::from_ref(&first),
            "a".into(),
            first.session.unwrap(),
            script(),
            Config::default(),
        )
        .unwrap();
    let call = owner
        .invoke(&[first], "a", 1, "input".into(), "old".into())
        .unwrap();
    let completed = call.wait().await.unwrap();
    let successor = device(1, 11);
    assert!(matches!(
        owner.accept(std::slice::from_ref(&successor), completed),
        Err(Error::Stale)
    ));
    assert!(matches!(
        owner.attach(
            std::slice::from_ref(&successor),
            "a".into(),
            successor.session.unwrap(),
            script(),
            Config::default()
        ),
        Err(Error::Busy)
    ));
    owner.reap().await.unwrap();
    owner
        .attach(
            std::slice::from_ref(&successor),
            "a".into(),
            successor.session.unwrap(),
            script(),
            Config::default(),
        )
        .unwrap();
    let completion = owner
        .invoke(
            &[successor],
            "a",
            1,
            "input".into(),
            "old incarnation".into(),
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert!(matches!(
        owner.accept(&[device(2, 12)], completion),
        Err(Error::Stale)
    ));
    owner.shutdown().await.unwrap();
}
#[tokio::test]
async fn reload_fences_completed_results_and_preserves_opaque_output_order() {
    let mut owner = Owner::new(1).unwrap();
    let devices = vec![device(1, 10)];
    owner
        .attach(
            &devices,
            "a".into(),
            devices[0].session.unwrap(),
            script(),
            Config::default(),
        )
        .unwrap();
    let old = owner
        .invoke(&devices, "a", 1, "input".into(), "old".into())
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        owner
            .reload(&devices, "a", 1, script())
            .unwrap()
            .wait()
            .await
            .unwrap(),
        2
    );
    assert!(matches!(owner.accept(&devices, old), Err(Error::Stale)));
    let new = owner
        .invoke(&devices, "a", 2, "input".into(), "opaque".into())
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(
        owner.accept(&devices, new).unwrap().outputs,
        vec![
            Output::Publish("opaque".into()),
            Output::Send("opaque".into())
        ]
    );
    owner.shutdown().await.unwrap();
}

#[tokio::test]
async fn reattachment_of_same_session_cannot_accept_previous_workers_result() {
    let mut owner = Owner::new(1).unwrap();
    let devices = vec![device(1, 10)];
    owner
        .attach(
            &devices,
            "a".into(),
            devices[0].session.unwrap(),
            script(),
            Config::default(),
        )
        .unwrap();
    let completed = owner
        .invoke(&devices, "a", 1, "input".into(), "previous worker".into())
        .unwrap()
        .wait()
        .await
        .unwrap();
    owner.reconcile(&[]);
    owner.reap().await.unwrap();
    owner
        .attach(
            &devices,
            "a".into(),
            devices[0].session.unwrap(),
            script(),
            Config::default(),
        )
        .unwrap();
    assert!(matches!(
        owner.accept(&devices, completed),
        Err(Error::Stale)
    ));
    owner.shutdown().await.unwrap();
}
#[tokio::test]
async fn removal_invalidates_even_completed_results_and_offline_grace_cannot_attach() {
    let mut owner = Owner::new(1).unwrap();
    let mut current = device(1, 10);
    owner
        .attach(
            std::slice::from_ref(&current),
            "a".into(),
            current.session.unwrap(),
            script(),
            Config::default(),
        )
        .unwrap();
    let completed = owner
        .invoke(
            std::slice::from_ref(&current),
            "a",
            1,
            "input".into(),
            "quiesced".into(),
        )
        .unwrap()
        .wait()
        .await
        .unwrap();
    current.removal = Some(Removal::Active {
        operation: 1,
        step: Step::Close,
    });
    assert!(matches!(
        owner.accept(std::slice::from_ref(&current), completed),
        Err(Error::Stale)
    ));
    owner.reap().await.unwrap();
    current.removal = None;
    let old_session = current.session.take().unwrap();
    assert!(current.online);
    assert!(matches!(
        owner.attach(
            &[current],
            "a".into(),
            old_session,
            script(),
            Config::default()
        ),
        Err(Error::Stale)
    ));
    owner.shutdown().await.unwrap();
}
