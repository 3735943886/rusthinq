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
    // Known old or future generations must fail admission, before using a
    // worker queue slot or producing a completion for the application to drop.
    for generation in [1, 3] {
        assert!(matches!(
            owner.invoke(&devices, "a", generation, "input".into(), "stale".into()),
            Err(Error::Stale)
        ));
    }
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

#[test]
fn configured_driver_cloud_callback_parses_envelope_and_is_optional() {
    let directory = tempfile::tempdir().unwrap();
    let config = rusthinq_scripting::drivers::Config {
        watch: false,
        directory: directory.path().into(),
        topic_prefix: "test".into(),
        bindings: Default::default(),
    };
    for source in [
        "fn on_cloud_event(ctx,event) { ctx.publish(event.topic + ':' + event.payload.value); }",
        "fn start(ctx) {}",
    ] {
        std::fs::write(directory.path().join("model.rhai"), source).unwrap();
        let compiled = config.prepare("d", "model", true, true).unwrap();
        let mut host = rusthinq_scripting::Host::new(compiled);
        let outcome = host.invoke(
            1,
            "__cloud",
            r#"{"topic":"lg/event","payload":{"value":"done"}}"#,
        );
        assert_eq!(outcome.error, None);
        if source.contains("on_cloud_event") {
            assert_eq!(
                outcome.outputs,
                vec![rusthinq_scripting::Output::Publish("lg/event:done".into())]
            );
        } else {
            assert!(outcome.outputs.is_empty());
        }
    }
}
