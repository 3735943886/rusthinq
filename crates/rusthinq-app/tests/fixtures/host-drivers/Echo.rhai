// Synthetic host integration fixture; no appliance interpretation.
fn start(ctx) {
    ctx.publish_raw(ctx.device_topic()+"/available", "true", true);
    ctx.publish_raw(ctx.device_topic()+"/status", "initial", true);
}
fn on_data(ctx, bytes) {
    ctx.publish_raw(ctx.device_topic()+"/status", bytes_utf8(bytes), true);
}
fn on_set_property(ctx, prop, value) {
    ctx.publish_raw(ctx.device_topic()+"/reject", `{"code":"unknown_property"}`, false);
}
fn on_drop(ctx) {
    ctx.publish_raw(ctx.device_topic()+"/available", "false", true);
}
