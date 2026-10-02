use rusthinq_scripting::{Compiled, Error, Host, Limits, Output, context::Config, modules::Source};
fn module(name: &str, source: &str) -> Source {
    Source {
        name: name.into(),
        source: source.into(),
    }
}
fn compiled(source: &str) -> Compiled {
    Compiled::new(source, Limits::default(), true).unwrap()
}

#[test]
fn ordered_dependencies_and_context_helpers_execute_with_opaque_outputs() {
    let source = r#"import "adapter" as a; fn input(ctx,v){a::output(ctx,v);}"#;
    let compiled = Compiled::with_context(
        source,
        Limits::default(),
        true,
        Config::new("d".into(), "m".into()),
    )
    .unwrap()
    .with_modules(vec![
        module("base", "fn decorate(v){\"opaque:\"+v}"),
        module(
            "adapter",
            r#"import "base" as b; fn output(ctx,v){ctx.publish(b::decorate(v));ctx.send(v);}"#,
        ),
    ])
    .unwrap();
    let mut host = Host::new(compiled);
    let outcome = host.invoke(1, "input", "not-json");
    assert_eq!(outcome.error, None);
    assert_eq!(
        outcome.outputs,
        vec![
            Output::Publish("opaque:not-json".into()),
            Output::Send("not-json".into())
        ]
    );
}
#[test]
fn module_bundle_is_bounded_and_flat_names_cannot_be_filesystem_paths() {
    for sources in [
        vec![module("/etc/passwd", "")],
        vec![module("../secret", "")],
        vec![module("dup", ""), module("dup", "")],
        (0..17).map(|i| module(&format!("m{i}"), "")).collect(),
    ] {
        assert!(matches!(
            compiled("fn input(v){}").with_modules(sources),
            Err(Error::InvalidConfig)
        ));
    }
    let base = Compiled::new(
        "fn input(v){}",
        Limits {
            source_bytes: 32,
            ..Limits::default()
        },
        true,
    )
    .unwrap();
    assert!(matches!(
        base.with_modules(vec![module("large", &" ".repeat(32))]),
        Err(Error::InvalidConfig)
    ));
    let ready = compiled("fn input(v){}").with_modules(vec![]).unwrap();
    assert!(matches!(
        ready.with_modules(vec![]),
        Err(Error::InvalidConfig)
    ));
}
#[test]
fn preparation_blocks_output_filesystem_imports_and_unbounded_initialization() {
    for source in [
        "publish(\"hidden\");",
        "send(\"hidden\");",
        "import \"/etc/passwd\" as p;",
        "loop {}",
    ] {
        let candidate = compiled("fn input(v){}").with_modules(vec![module("invalid", source)]);
        assert!(
            matches!(candidate, Err(Error::Compile(_))),
            "source: {source}"
        );
    }
    assert!(matches!(
        compiled("fn input(v){}").with_modules(vec![
            module("first", "import \"later\" as l;"),
            module("later", "")
        ]),
        Err(Error::Compile(_))
    ));
    let mut host = Host::new(
        compiled("import \"/etc/passwd\" as p; fn input(v){}")
            .with_modules(vec![])
            .unwrap(),
    );
    assert!(matches!(
        host.invoke(1, "input", "").error,
        Some(Error::Execution(_))
    ));
}
#[test]
fn module_preparation_failure_cannot_replace_running_generation_and_reload_uses_new_bundle() {
    let entry = "import \"helper\" as h; fn input(v){publish(h::value());}";
    let mut host = Host::new(
        compiled(entry)
            .with_modules(vec![module("helper", "fn value(){\"old\"}")])
            .unwrap(),
    );
    assert!(
        compiled(entry)
            .with_modules(vec![module("helper", "fn broken(")])
            .is_err()
    );
    assert_eq!(
        host.invoke(1, "input", "").outputs,
        vec![Output::Publish("old".into())]
    );
    host.reload(
        compiled(entry)
            .with_modules(vec![module("helper", "fn value(){\"new\"}")])
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        host.invoke(2, "input", "").outputs,
        vec![Output::Publish("new".into())]
    );
}
#[test]
fn pinned_real_aabb_common_pure_helpers_run_without_legacy_host_il_apis() {
    // rusthinq-scripts 54292921c6edc72ea6ec901b1137845bff14e6ae, unchanged source.
    let source = r#"import "aabb_common" as c; fn input(v){publish(c::bit(8,8));publish(c::bit(8,1));publish(c::on_off(1));}"#;
    let mut host = Host::new(
        compiled(source)
            .with_modules(vec![module(
                "aabb_common",
                include_str!("fixtures/aabb_common.rhai"),
            )])
            .unwrap(),
    );
    let result = host.invoke(1, "input", "");
    assert_eq!(result.error, None);
    assert_eq!(
        result.outputs,
        vec![
            Output::Publish("true".into()),
            Output::Publish("false".into()),
            Output::Publish("true".into())
        ]
    );
}

#[test]
fn module_imports_do_not_repeat_initialization_or_reset_mutable_scope() {
    let source = r#"import "helper" as h; let count=0;publish("init");
        fn input(v){count+=1;publish(h::show(count));}"#;
    let mut host = Host::new(
        compiled(source)
            .with_modules(vec![module("helper", "fn show(v){v.to_string()}")])
            .unwrap(),
    );
    let first = host.invoke(1, "input", "");
    assert_eq!(first.error, None);
    assert_eq!(
        first.outputs,
        vec![Output::Publish("init".into()), Output::Publish("1".into())]
    );
    let second = host.invoke(1, "input", "");
    assert_eq!(second.error, None);
    assert_eq!(second.outputs, vec![Output::Publish("2".into())]);
}

#[test]
fn entry_added_after_module_preparation_uses_the_same_import_environment() {
    let compiled = Compiled::with_context(
        "import \"helper\" as h; fn original(ctx,v){ctx.send(h::value(v));}",
        Limits::default(),
        true,
        Config::new("d".into(), "m".into()),
    )
    .unwrap()
    .with_modules(vec![module("helper", "fn value(v){\"module:\"+v}")])
    .unwrap()
    .with_entry("fn added(ctx,v){original(ctx,v);}")
    .unwrap();
    let result = Host::new(compiled).invoke(1, "added", "value");
    assert_eq!(result.error, None);
    assert_eq!(result.outputs, vec![Output::Send("module:value".into())]);
}
