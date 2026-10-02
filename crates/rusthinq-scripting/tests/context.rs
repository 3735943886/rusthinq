use rusthinq_scripting::{Compiled, Error, Host, Limits, Output, context::Config};
fn compiled(source: &str, device: &str) -> Compiled {
    Compiled::with_context(
        source,
        Limits::default(),
        true,
        Config::new(device.into(), "model".into()),
    )
    .unwrap()
}
#[test]
fn context_callbacks_keep_device_state_and_opaque_outputs_without_semantic_helpers() {
    let source = r#"fn on_response(ctx, body) {
        let state=ctx.state_get("state");
        if state == () {state=#{count:0,values:[true,1,"raw"]};}
        state.count+=1;ctx.state_set("state",state);
        ctx.publish(ctx.id()+":"+ctx.model_id()+":"+state.count.to_string());
        ctx.send_json(body);
    }"#;
    let mut first = Host::new(compiled(source, "first"));
    let mut second = Host::new(compiled(source, "second"));
    for count in 1..=2 {
        let result = first.invoke(1, "on_response", "opaque not JSON");
        assert_eq!(result.error, None);
        assert_eq!(
            result.outputs,
            vec![
                Output::Publish(format!("first:model:{count}")),
                Output::Send("opaque not JSON".into())
            ]
        );
    }
    assert_eq!(
        second.invoke(1, "on_response", "x").outputs[0],
        Output::Publish("second:model:1".into())
    );
    first.reload(compiled(source, "first")).unwrap();
    assert_eq!(
        first.invoke(2, "on_response", "fresh").outputs[0],
        Output::Publish("first:model:1".into())
    );
}
#[test]
fn context_state_is_bounded_and_cannot_store_context_or_shared_aliases() {
    let mut config = Config::new("d".into(), "m".into());
    config.state_keys = 1;
    config.state_bytes = 40;
    let source = r#"fn input(ctx,v){ctx.state_set("a","old");ctx.publish("prefix");ctx.state_set("b","new");}"#;
    let mut host =
        Host::new(Compiled::with_context(source, Limits::default(), true, config).unwrap());
    let result = host.invoke(1, "input", "");
    assert_eq!(result.outputs, vec![Output::Publish("prefix".into())]);
    assert!(
        matches!(result.error,Some(Error::Execution(reason)) if reason.contains("state capacity"))
    );
    for source in [
        "fn input(ctx,v){ctx.state_set(\"self\",ctx);}",
        "fn input(ctx,v){ctx.state_set(\"function\",||42);}",
    ] {
        let mut host = Host::new(compiled(source, "d"));
        assert!(matches!(
            host.invoke(1, "input", "").error,
            Some(Error::Execution(_))
        ));
    }
}
#[test]
fn state_replacement_removal_and_get_clones_do_not_bypass_accounting() {
    let source = r#"fn input(ctx,v){
        ctx.state_set("a",#{count:1});let value=ctx.state_get("a");value.count=99;
        ctx.publish(ctx.state_get("a").count.to_string());
        ctx.state_set("a",#{count:2});ctx.publish(ctx.state_has("a").to_string());
        ctx.state_remove("a");ctx.publish(ctx.state_has("a").to_string());
        ctx.state_set("b",#{count:3});ctx.publish(ctx.state_get("b").count.to_string());
    }"#;
    let mut config = Config::new("d".into(), "m".into());
    config.state_keys = 1;
    config.state_bytes = 64;
    let mut host =
        Host::new(Compiled::with_context(source, Limits::default(), true, config).unwrap());
    let result = host.invoke(1, "input", "");
    assert_eq!(result.error, None);
    assert_eq!(
        result.outputs,
        vec![
            Output::Publish("1".into()),
            Output::Publish("true".into()),
            Output::Publish("false".into()),
            Output::Publish("3".into())
        ]
    );
}

#[test]
fn shared_script_variable_is_flattened_to_an_independent_value_before_storage() {
    let source = r#"fn input(ctx,v){let a=[1];let f=||a;
        ctx.state_set("alias",a);a.push(2);
        ctx.publish(ctx.state_get("alias").len().to_string());}"#;
    let mut host = Host::new(compiled(source, "d"));
    let result = host.invoke(1, "input", "");
    assert_eq!(result.error, None);
    assert_eq!(result.outputs, vec![Output::Publish("1".into())]);
}
#[test]
fn context_publication_uses_same_consumer_and_output_bounds_as_plain_host() {
    let mut compiled = compiled("fn input(ctx,v){ctx.publish(v);ctx.send(v);}", "d");
    compiled.set_consumer_enabled(false);
    let mut host = Host::new(compiled);
    assert!(
        matches!(host.invoke(1,"input","x").error,Some(Error::Execution(reason)) if reason.contains("consumer disabled"))
    );
    let mut host = Host::new(
        Compiled::with_context(
            "fn input(ctx,v){ctx.send(v);ctx.publish(v);}",
            Limits {
                outputs: 1,
                ..Limits::default()
            },
            true,
            Config::new("d".into(), "m".into()),
        )
        .unwrap(),
    );
    let result = host.invoke(1, "input", "opaque");
    assert_eq!(result.outputs, vec![Output::Send("opaque".into())]);
    assert!(matches!(result.error, Some(Error::Execution(_))));
}
