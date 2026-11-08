// Synthetic host integration fixture; no appliance interpretation.
fn start(ctx) {
    ctx.publish_raw(ctx.device_topic()+"/available", "true", true);
}
fn on_drop(ctx) {
    ctx.publish_raw(ctx.device_topic()+"/available", "false", true);
}
