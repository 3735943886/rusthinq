use rusthinq_lifecycle::*;
use std::time::Duration;

fn model() -> Model {
    Model::new(Ledger::default(), Duration::from_secs(10), Duration::ZERO).unwrap()
}
fn up(model: &mut Model, id: &str, generation: u64, now: u64) -> Outcome {
    let outcome = model.input(
        Input::SessionUp {
            id: id.into(),
            generation,
        },
        Duration::from_secs(now),
    );
    for action in &outcome.actions {
        if let Action::PersistLedger(ledger) = action {
            model.input(
                Input::LedgerResult {
                    revision: ledger.revision,
                    result: Ok(()),
                },
                Duration::from_secs(now),
            );
        }
    }
    outcome
}
fn key(model: &Model, id: &str) -> SessionKey {
    model
        .devices()
        .into_iter()
        .find(|d| d.entry.id == id)
        .unwrap()
        .session
        .unwrap()
}
fn step(outcome: &Outcome) -> (String, u64, u64, Step) {
    outcome
        .actions
        .iter()
        .find_map(|a| {
            if let Action::ForgetStep {
                id,
                incarnation,
                operation,
                step,
                ..
            } = a
            {
                Some((id.clone(), *incarnation, *operation, *step))
            } else {
                None
            }
        })
        .expect("expected step effect")
}
fn finish(model: &mut Model, effect: &Outcome, result: Result<(), String>) -> Outcome {
    let (id, incarnation, operation, step) = step(effect);
    model.input(
        Input::StepResult {
            id,
            incarnation,
            operation,
            step,
            result,
        },
        Duration::ZERO,
    )
}
fn forget(model: &mut Model, bridge_active: bool) -> Outcome {
    model.input(
        Input::Forget {
            id: "dev".into(),
            bridge_active,
        },
        Duration::ZERO,
    )
}

#[test]
fn replacement_ignores_late_close_and_stale_generations() {
    let mut m = model();
    up(&mut m, "dev", 1, 0);
    let old = key(&m, "dev");
    let next = up(&mut m, "dev", 2, 0);
    assert_eq!(
        next.actions[0],
        Action::CloseSuperseded {
            id: "dev".into(),
            session: old
        }
    );
    assert!(
        !next
            .actions
            .iter()
            .any(|a| matches!(a, Action::Online { .. }))
    );
    let current = key(&m, "dev");
    assert!(
        m.input(
            Input::SessionDown {
                id: "dev".into(),
                session: old
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
    assert_eq!(key(&m, "dev"), current);
    assert_eq!(up(&mut m, "dev", 1, 0).error, Some(Error::StaleGeneration));
}

#[test]
fn grace_delays_only_online_report_and_reconnect_cancels_it() {
    let mut m = model();
    up(&mut m, "dev", 1, 0);
    let session = key(&m, "dev");
    let down = m.input(
        Input::SessionDown {
            id: "dev".into(),
            session,
        },
        Duration::from_secs(1),
    );
    assert_eq!(down.next_deadline, Some(Duration::from_secs(11)));
    assert!(m.devices()[0].session.is_none());
    assert!(m.devices()[0].online);
    assert!(
        up(&mut m, "dev", 2, 10)
            .actions
            .iter()
            .all(|a| !matches!(a, Action::Online { .. }))
    );
    assert!(
        m.input(Input::Tick, Duration::from_secs(11))
            .actions
            .is_empty()
    );
    let session = key(&m, "dev");
    m.input(
        Input::SessionDown {
            id: "dev".into(),
            session,
        },
        Duration::from_secs(12),
    );
    let reconnect = up(&mut m, "dev", 3, 22);
    assert!(matches!(reconnect.actions[0], Action::Offline { .. }));
    assert!(matches!(reconnect.actions[2], Action::Online { .. }));
}

#[test]
fn local_forget_quiesces_closes_and_removes_only_after_durable_success() {
    let mut m = model();
    up(&mut m, "dev", 1, 0);
    let session = key(&m, "dev");
    let close = forget(&mut m, false);
    assert_eq!(step(&close).3, Step::Close);
    assert!(!m.devices()[0].online);
    assert!(m.devices()[0].session.is_none());
    assert!(
        close
            .actions
            .iter()
            .any(|a| matches!(a,Action::ForgetStep {session:Some(s),..} if *s==session))
    );
    assert_eq!(forget(&mut m, false).error, Some(Error::Busy));
    assert_eq!(up(&mut m, "dev", 2, 0).error, Some(Error::Quiesced));
    let persist = finish(&mut m, &close, Ok(()));
    assert_eq!(step(&persist).3, Step::PersistRemoval);
    assert!(persist.actions.iter().any(|a| matches!(a,Action::ForgetStep {ledger:Some(l),..} if l.entries.is_empty() && l.next_incarnation==2)));
    assert_eq!(m.devices().len(), 1);
    let removed = finish(&mut m, &persist, Ok(()));
    assert_eq!(
        removed.actions,
        vec![Action::Removed {
            id: "dev".into(),
            incarnation: 1
        }]
    );
    assert!(m.devices().is_empty());
    up(&mut m, "dev", 1, 0);
    assert_eq!(key(&m, "dev").incarnation, 2);
    assert!(finish(&mut m, &persist, Ok(())).actions.is_empty());
    assert!(
        m.input(
            Input::SessionDown {
                id: "dev".into(),
                session
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
    assert!(m.devices()[0].online);
}

#[test]
fn every_failure_stays_quiesced_and_retries_only_on_request() {
    for failed in [Step::Close, Step::Deregister, Step::PersistRemoval] {
        let mut m = model();
        up(&mut m, "dev", 1, 0);
        let mut effect = forget(&mut m, true);
        while step(&effect).3 != failed {
            effect = finish(&mut m, &effect, Ok(()));
        }
        let failure = finish(&mut m, &effect, Err("storage/transport failure".into()));
        assert!(matches!(failure.actions[0],Action::ForgetFailed {step,..} if step==failed));
        assert!(matches!(m.devices()[0].removal,Some(Removal::Failed {step,..}) if step==failed));
        assert!(m.devices()[0].session.is_none());
        assert!(!m.devices()[0].online);
        assert!(m.input(Input::Tick, Duration::ZERO).actions.is_empty());
        assert!(finish(&mut m, &effect, Ok(())).actions.is_empty());
        let retry = forget(&mut m, false);
        assert_eq!(step(&retry).3, failed);
        assert_ne!(step(&retry).2, step(&effect).2);
        // The bridge choice is captured by the initial request, not changed by retry.
        let success = finish(&mut m, &retry, Ok(()));
        if failed == Step::Close {
            assert_eq!(step(&success).3, Step::Deregister);
        }
    }
}

#[test]
fn unrelated_device_progress_and_correlated_results() {
    let mut m = model();
    up(&mut m, "dev", 1, 0);
    let close = forget(&mut m, true);
    assert_eq!(up(&mut m, "other", 1, 0).error, None);
    let (id, incarnation, operation, _) = step(&close);
    for (inc, op, s) in [
        (incarnation + 1, operation, Step::Close),
        (incarnation, operation + 1, Step::Close),
        (incarnation, operation, Step::Deregister),
    ] {
        assert!(
            m.input(
                Input::StepResult {
                    id: id.clone(),
                    incarnation: inc,
                    operation: op,
                    step: s,
                    result: Ok(())
                },
                Duration::ZERO
            )
            .actions
            .is_empty()
        );
    }
    assert_eq!(step(&finish(&mut m, &close, Ok(()))).3, Step::Deregister);
}

#[test]
fn ledger_loading_and_failed_writes_are_explicit() {
    let mut m = model();
    let outcome = m.input(
        Input::SessionUp {
            id: "dev".into(),
            generation: 7,
        },
        Duration::ZERO,
    );
    let ledger = outcome
        .actions
        .iter()
        .find_map(|a| {
            if let Action::PersistLedger(l) = a {
                Some(l.clone())
            } else {
                None
            }
        })
        .unwrap();
    let revision = ledger.revision;
    let failed = m.input(
        Input::LedgerResult {
            revision,
            result: Err("disk full".into()),
        },
        Duration::ZERO,
    );
    assert_eq!(
        failed.actions,
        vec![Action::LedgerResult {
            revision,
            result: Err("disk full".into())
        }]
    );
    assert!(m.devices()[0].online);
    assert!(
        m.input(
            Input::LedgerResult {
                revision,
                result: Ok(())
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
    let mut loaded = Model::new(ledger, Duration::from_secs(10), Duration::ZERO).unwrap();
    assert!(!loaded.devices()[0].online);
    assert!(loaded.devices()[0].session.is_none());
    assert_eq!(
        up(&mut loaded, "dev", 7, 0).error,
        Some(Error::StaleGeneration)
    );
    assert_eq!(up(&mut loaded, "dev", 8, 0).error, None);
}

#[test]
fn earliest_deadline_zero_grace_and_backwards_time() {
    let mut m = model();
    up(&mut m, "a", 1, 0);
    up(&mut m, "b", 1, 0);
    for (id, now) in [("a", 1), ("b", 2)] {
        let session = key(&m, id);
        m.input(
            Input::SessionDown {
                id: id.into(),
                session,
            },
            Duration::from_secs(now),
        );
    }
    let before = m.devices();
    assert_eq!(
        m.input(Input::Tick, Duration::from_secs(1)).error,
        Some(Error::TimeWentBackwards)
    );
    assert_eq!(m.devices(), before);
    assert_eq!(
        m.input(Input::Tick, Duration::from_secs(11)).next_deadline,
        Some(Duration::from_secs(12))
    );
    assert_eq!(
        m.input(Input::Tick, Duration::from_secs(12)).next_deadline,
        None
    );
    let mut m = Model::new(Ledger::default(), Duration::ZERO, Duration::ZERO).unwrap();
    up(&mut m, "dev", 1, 0);
    let session = key(&m, "dev");
    let down = m.input(
        Input::SessionDown {
            id: "dev".into(),
            session,
        },
        Duration::ZERO,
    );
    assert!(
        down.actions
            .iter()
            .any(|a| matches!(a, Action::Offline { .. }))
    );
    assert_eq!(down.next_deadline, None);
}

#[test]
fn invalid_ledgers_and_counter_exhaustion_do_not_mutate_state() {
    let entry = Entry {
        id: "dev".into(),
        incarnation: 1,
        last_generation: 1,
    };
    for entries in [
        vec![entry.clone(), entry.clone()],
        vec![Entry {
            incarnation: 0,
            ..entry.clone()
        }],
        vec![Entry {
            id: "".into(),
            ..entry.clone()
        }],
    ] {
        assert!(matches!(
            Model::new(
                Ledger {
                    revision: 0,
                    next_incarnation: 2,
                    entries
                },
                Duration::ZERO,
                Duration::ZERO
            ),
            Err(Error::InvalidLedger)
        ));
    }
    let mut m = Model::new(
        Ledger {
            revision: u64::MAX,
            ..Ledger::default()
        },
        Duration::ZERO,
        Duration::ZERO,
    )
    .unwrap();
    assert_eq!(up(&mut m, "dev", 1, 0).error, Some(Error::CounterExhausted));
    assert!(m.devices().is_empty());
    assert_eq!(
        up(&mut model(), "", 1, 0).error,
        Some(Error::InvalidIdentity)
    );
    assert_eq!(
        forget(&mut model(), false).error,
        Some(Error::UnknownDevice)
    );
}

#[test]
fn removal_failure_is_not_committed_by_an_unrelated_write() {
    let mut m = model();
    up(&mut m, "dev", 1, 0);
    let close = forget(&mut m, false);
    let persist = finish(&mut m, &close, Ok(()));
    let update = up(&mut m, "other", 1, 0);
    assert!(
        !update
            .actions
            .iter()
            .any(|a| matches!(a, Action::PersistLedger(_)))
    );
    let failed = finish(&mut m, &persist, Err("disk full".into()));
    let ledger = failed
        .actions
        .iter()
        .find_map(|a| {
            if let Action::PersistLedger(l) = a {
                Some(l)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(ledger.entries.len(), 2);
    assert!(ledger.entries.iter().any(|e| e.id == "dev"));
    assert!(
        m.devices()
            .iter()
            .any(|d| d.entry.id == "dev" && matches!(d.removal, Some(Removal::Failed { .. })))
    );
}

#[test]
fn slow_storage_coalesces_updates_and_ignores_unissued_results() {
    let mut m = model();
    let first = m.input(
        Input::SessionUp {
            id: "dev".into(),
            generation: 1,
        },
        Duration::ZERO,
    );
    let revision = first
        .actions
        .iter()
        .find_map(|a| {
            if let Action::PersistLedger(l) = a {
                Some(l.revision)
            } else {
                None
            }
        })
        .unwrap();
    for generation in 2..1000 {
        let next = m.input(
            Input::SessionUp {
                id: "dev".into(),
                generation,
            },
            Duration::ZERO,
        );
        assert!(
            !next
                .actions
                .iter()
                .any(|a| matches!(a, Action::PersistLedger(_)))
        );
    }
    let close = forget(&mut m, false);
    let waiting = finish(&mut m, &close, Ok(()));
    assert!(!waiting.actions.iter().any(|a| matches!(
        a,
        Action::ForgetStep {
            step: Step::PersistRemoval,
            ..
        }
    )));
    let device = &m.devices()[0];
    let Some(Removal::Active {
        operation,
        step: Step::PersistRemoval,
    }) = device.removal
    else {
        panic!("waiting removal")
    };
    let early = m.input(
        Input::StepResult {
            id: "dev".into(),
            incarnation: device.entry.incarnation,
            operation,
            step: Step::PersistRemoval,
            result: Ok(()),
        },
        Duration::ZERO,
    );
    assert!(early.actions.is_empty());
    let next = m.input(
        Input::LedgerResult {
            revision,
            result: Ok(()),
        },
        Duration::ZERO,
    );
    assert_eq!(step(&next).3, Step::PersistRemoval);
    assert!(
        m.input(
            Input::LedgerResult {
                revision,
                result: Ok(())
            },
            Duration::ZERO
        )
        .actions
        .is_empty()
    );
    assert!(
        finish(&mut m, &next, Ok(()))
            .actions
            .iter()
            .any(|a| matches!(a, Action::Removed { .. }))
    );
}

#[test]
fn concurrent_removals_get_independent_sequential_snapshots() {
    let mut m = model();
    up(&mut m, "dev", 1, 0);
    up(&mut m, "other", 1, 0);
    let close = forget(&mut m, false);
    let first = finish(&mut m, &close, Ok(()));
    let other = m.input(
        Input::Forget {
            id: "other".into(),
            bridge_active: false,
        },
        Duration::ZERO,
    );
    let waiting = finish(&mut m, &other, Ok(()));
    assert!(!waiting.actions.iter().any(|a| matches!(
        a,
        Action::ForgetStep {
            step: Step::PersistRemoval,
            ..
        }
    )));
    let next = finish(&mut m, &first, Err("disk full".into()));
    let second = next
        .actions
        .iter()
        .find_map(|a| {
            if let Action::ForgetStep {
                ledger: Some(l), ..
            } = a
            {
                Some(l)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].id, "dev");
    finish(&mut m, &next, Ok(()));
    assert_eq!(m.devices().len(), 1);
    assert_eq!(m.devices()[0].entry.id, "dev");
}
