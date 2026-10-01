//! Load LG ThinQ OAuth credentials and list homes/devices via the ThinQ API.
//! Replaces tools/lgcloud-monitor.ts for the common RE use case of confirming
//! cloud account access. Live MQTT notification streaming is handled by
//! rusthinq-cloud bridge mode while devices are bridged.
//!
//! Usage:
//!   lgcloud-monitor [--state DIR]
//!
//! Expects oauth2.json from bridge storage (management UI login).

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use rusthinq_bridge::state::{BridgeState, Environment, JsonStorage};
use rusthinq_bridge::thinq_api::Client;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "lgcloud-monitor")]
struct Args {
    /// Directory containing oauth2.json (rusthinq-cloud bridge storage_path)
    #[arg(long, default_value = "./state")]
    state: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let storage = JsonStorage::new(&args.state);
    let creds = storage.get_credentials().ok_or_else(|| {
        anyhow!(
            "No oauth2.json in {}. Log in via rusthinq management UI \
             (Bridge → Log into LG account), then pass that storage path with --state.",
            args.state.display()
        )
    })?;

    let env = Environment {
        country_code: creds.env.country_code.clone(),
        language_code: creds.env.language_code.clone(),
    };
    eprintln!(
        "[lgcloud-monitor] country={} — authenticating…",
        env.country_code
    );

    let mut client = Client::new(env);
    client
        .auth(&creds.refresh_token)
        .await
        .context("ThinQ auth (refresh token may be expired — re-login via management UI)")?;

    println!(
        "{}",
        serde_json::json!({
            "ok": true,
            "countryCode": creds.env.country_code,
            "clientId": client.client_id,
            "homeId": client.home_id,
            "note": "For live cloud MQTT notifications while reverse-engineering, enable bridge mode on the device in the management UI. This tool validates OAuth and home binding."
        })
    );
    Ok(())
}
