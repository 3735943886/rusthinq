use rusthinq_scripting::{Compiled, Error, Host, Limits, Output};
fn compiled(source: &str) -> Compiled {
    Compiled::new(source, Limits::default(), true).unwrap()
}
#[test]
fn opaque_outputs_preserve_order_and_global_state_without_il_interpretation() {
    let mut host = Host::new(compiled(
        "let n=0; fn input(value) { n+=1; publish(value); send(n.to_string()); }",
    ));
    for count in [1, 2] {
        let result = host.invoke(1, "input", "opaque-not-json");
        assert_eq!(result.error, None);
        assert_eq!(
            result.outputs,
            vec![
                Output::Publish("opaque-not-json".into()),
                Output::Send(count.to_string())
            ]
        );
    }
}
#[test]
fn output_limit_fault_preserves_prefix_and_requires_explicit_reload() {
    let limits = Limits {
        outputs: 1,
        ..Limits::default()
    };
    let mut host =
        Host::new(Compiled::new("fn input(v) {publish(v);send(v);}", limits, true).unwrap());
    let result = host.invoke(1, "input", "x");
    assert!(matches!(result.error, Some(Error::Execution(_))));
    assert_eq!(result.outputs, vec![Output::Publish("x".into())]);
    assert_eq!(host.invoke(1, "input", "x").error, Some(Error::Faulted));
    assert!(Compiled::new("fn broken(", Limits::default(), true).is_err());
    assert_eq!(host.generation(), 1);
    host.reload(compiled("fn input(v) {send(v);}")).unwrap();
    assert_eq!(host.invoke(1, "input", "late").error, Some(Error::Stale));
    assert_eq!(
        host.invoke(2, "input", "fresh").outputs,
        vec![Output::Send("fresh".into())]
    );
}
#[test]
fn disabled_consumer_and_operation_limits_fail_explicitly_without_affecting_other_host() {
    let mut disabled =
        Host::new(Compiled::new("fn input(v) {publish(v);}", Limits::default(), false).unwrap());
    assert!(
        matches!(disabled.invoke(1,"input","x").error,Some(Error::Execution(message)) if message.contains("consumer disabled"))
    );
    let mut looping = Host::new(
        Compiled::new(
            "fn input(v) {loop {}}",
            Limits {
                operations: 100,
                ..Limits::default()
            },
            true,
        )
        .unwrap(),
    );
    assert!(matches!(
        looping.invoke(1, "input", "").error,
        Some(Error::Execution(_))
    ));
    let mut other = Host::new(compiled("fn input(v) {send(v);}"));
    assert_eq!(other.invoke(1, "input", "ok").error, None);
}
#[test]
fn imports_cannot_read_files_and_input_is_bounded() {
    let mut host = Host::new(compiled("import \"/etc/passwd\" as p; fn input(v) {}"));
    assert!(matches!(
        host.invoke(1, "input", "").error,
        Some(Error::Execution(_))
    ));
    let mut host = Host::new(
        Compiled::new(
            "fn input(v) {}",
            Limits {
                string_bytes: 4,
                ..Limits::default()
            },
            true,
        )
        .unwrap(),
    );
    assert!(matches!(
        host.invoke(1, "input", "12345").error,
        Some(Error::Execution(_))
    ));
}
