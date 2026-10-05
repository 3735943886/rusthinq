// Synthetic command callback for transport and generation-fencing tests.
fn start(ctx) { ctx.publish_raw(ctx.device_topic()+"/available", "true", true); }
fn on_set_property(ctx, prop, value) { ctx.send_raw(hex_decode(value)); }
