//! Find (and optionally clear) orphaned retained MQTT topics that rusthinq's own
//! `forget` command never got a chance to clean up -- e.g. a device that crashed,
//! got replaced, or was deregistered elsewhere without ever going through
//! `<prefix>/<id>/forget/set` (see rusthinq-cloud's `device_control.rs`). Nothing
//! in rusthinq itself discovers pre-existing retained messages on the broker, so
//! those stay forever unless something like this walks the broker directly.
//!
//! Method: fetch the retained `<prefix>/devices` snapshot (rusthinq-cloud's own
//! bookkeeping, which keeps a known-but-offline device listed too -- see
//! `devlist.rs`) for the set of ids rusthinq still knows about, then subscribe to
//! `<prefix>/+/+` (property topics, `MqttSink::publish_property`) and, if
//! `--il-prefix` is given, `<il_prefix>/+` (IL descriptors, `ctx.rs`'s
//! `publish_descriptor`) to see what's actually retained on the broker. Any id
//! found there but missing from `<prefix>/devices` is an orphan.
//!
//! Defaults to a dry run (report only); pass `--apply` to actually clear orphaned
//! topics, which then asks for an interactive "yes" unless `--yes` is also given
//! (for non-interactive/scripted use).
//!
//! Usage:
//!   rusthinq-retained-gc <mqtt-host[:port]> [--prefix rusthinq] [--il-prefix il] [--apply] [--yes]

use anyhow::{Result, anyhow};
use clap::Parser;
use rusthinq_tools::reconcile;
use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "rusthinq-retained-gc", verbatim_doc_comment)]
/// Find and (optionally) clear orphaned retained rusthinq MQTT topics.
///
/// Dry run by default -- prints what it found and changes nothing. Pass --apply
/// to actually clear the orphaned topics (with an interactive confirmation
/// unless --yes is also given).
struct Args {
    /// MQTT broker host[:port] (plain MQTT, no TLS)
    host: String,

    /// rusthinq [mqtt] rusthinq_prefix (default: $RUSTHINQ_PREFIX or "rusthinq")
    #[arg(long)]
    prefix: Option<String>,

    /// rusthinq [scripting] il_prefix, if configured -- omit to skip IL descriptor topics
    #[arg(long)]
    il_prefix: Option<String>,

    /// Actually clear orphaned retained topics (default: dry run / report only)
    #[arg(long)]
    apply: bool,

    /// Skip the interactive confirmation prompt when using --apply
    #[arg(long)]
    yes: bool,

    /// How long to wait for retained property/IL messages after subscribing
    #[arg(long, default_value_t = 3)]
    window_secs: u64,

    /// How long to wait for the <prefix>/devices retained snapshot
    #[arg(long, default_value_t = 3)]
    devices_timeout_secs: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let prefix = args
        .prefix
        .clone()
        .or_else(|| std::env::var("RUSTHINQ_PREFIX").ok())
        .unwrap_or_else(|| "rusthinq".to_string());
    let client_id = format!("rusthinq-retained-gc-{}", std::process::id());

    let devices_topic = format!("{prefix}/devices");
    eprintln!("[rusthinq-retained-gc] fetching {devices_topic} ...");
    let devices_payload = rusthinq_tools::mqtt::fetch_one(
        &format!("{client_id}-devices"),
        &args.host,
        &devices_topic,
        Duration::from_secs(args.devices_timeout_secs),
    )?
    .ok_or_else(|| {
        anyhow!(
            "no retained {devices_topic} found within {}s -- refusing to guess which \
             device ids are orphaned without it. Is rusthinq-cloud running and connected \
             to this broker, and is --prefix correct?",
            args.devices_timeout_secs
        )
    })?;
    let known_ids =
        reconcile::known_ids_from_devices_snapshot(&String::from_utf8_lossy(&devices_payload))?;
    eprintln!(
        "[rusthinq-retained-gc] {} known device id(s) in {devices_topic}",
        known_ids.len()
    );

    let mut orphan_topics: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let prop_filter = format!("{prefix}/+/+");
    eprintln!(
        "[rusthinq-retained-gc] scanning retained {prop_filter} (waiting {}s) ...",
        args.window_secs
    );
    let prop_msgs = rusthinq_tools::mqtt::collect_retained(
        &format!("{client_id}-props"),
        &args.host,
        &prop_filter,
        Duration::from_secs(args.window_secs),
    )?;
    for p in &prop_msgs {
        let topic = String::from_utf8_lossy(&p.topic);
        if let Some((id, _property)) = reconcile::parse_property_topic(&topic, &prefix)
            && !known_ids.contains(id)
        {
            orphan_topics
                .entry(id.to_string())
                .or_default()
                .push(topic.to_string());
        }
    }

    if let Some(il_prefix) = &args.il_prefix {
        let il_filter = format!("{il_prefix}/+");
        eprintln!(
            "[rusthinq-retained-gc] scanning retained {il_filter} (waiting {}s) ...",
            args.window_secs
        );
        let il_msgs = rusthinq_tools::mqtt::collect_retained(
            &format!("{client_id}-il"),
            &args.host,
            &il_filter,
            Duration::from_secs(args.window_secs),
        )?;
        for p in &il_msgs {
            let topic = String::from_utf8_lossy(&p.topic);
            if let Some(id) = reconcile::parse_il_topic(&topic, il_prefix)
                && !known_ids.contains(id)
            {
                orphan_topics
                    .entry(id.to_string())
                    .or_default()
                    .push(topic.to_string());
            }
        }
    }

    if orphan_topics.is_empty() {
        println!("no orphaned retained topics found -- nothing to do");
        return Ok(());
    }

    let total_topics: usize = orphan_topics.values().map(|v| v.len()).sum();
    println!(
        "found {} orphaned device id(s), {total_topics} retained topic(s):",
        orphan_topics.len()
    );
    for (id, topics) in &orphan_topics {
        println!("  {id}");
        for t in topics {
            println!("    {t}");
        }
    }

    if !args.apply {
        println!(
            "\ndry run -- nothing was changed. Re-run with --apply to clear these {total_topics} topic(s)."
        );
        return Ok(());
    }

    if !args.yes {
        print!(
            "\nAbout to permanently clear {total_topics} retained topic(s) on {}. Type 'yes' to continue: ",
            args.host
        );
        std::io::stdout().flush().ok();
        let mut answer = String::new();
        std::io::stdin().read_line(&mut answer)?;
        if answer.trim() != "yes" {
            println!("aborted -- nothing was changed");
            return Ok(());
        }
    }

    let all_topics: Vec<String> = orphan_topics.into_values().flatten().collect();
    rusthinq_tools::mqtt::clear_retained_topics(
        &format!("{client_id}-clear"),
        &args.host,
        &all_topics,
    )?;
    println!("cleared {} retained topic(s)", all_topics.len());
    Ok(())
}
