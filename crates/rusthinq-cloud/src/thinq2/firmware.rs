//! Where an appliance's firmware comes from.
//!
//! An appliance that reaches rusthinq by port redirection sends it every connection it
//! makes on 443, including ones rusthinq has no business answering. A firmware/SOTA
//! download is the case that matters: the image lives on a public CDN, and the
//! appliance checks that certificate against its built-in roots, not the CA it pinned
//! from `/route/certificate`, so a certificate rusthinq signs is refused and the
//! connection is dropped mid-handshake — before there is any request to answer.
//!
//! Those connections have to reach the real server instead (see sni_passthrough.rs),
//! and to route one, rusthinq has to know the name belongs to a download. Hardcoding a
//! CDN would only hold for the one region it was read off, and there is more than one
//! cmd shape that hands an appliance a URL to fetch on its own — `startFota` is the
//! original one, but SOTA app content does the same thing under a field never named
//! here, and likely differs by cmd/region. So this is learned two ways: proactively,
//! by `note_urls_in` scanning every cloud→device message for anything that parses as
//! an http(s) URL, whatever cmd or field carried it; and reactively, when a name
//! terminated here gets the exact rejection a firmware host produces — the appliance
//! resets the handshake because it wanted a real root, not ours — which needs no cmd
//! or field to be spotted at all, only for the appliance to have tried and failed once.
//!
//! Both of those are inferences from a single failure, and a single failure is not
//! always what it looks like: a caller that was never going to accept rusthinq's CA in
//! the first place produces the identical rejection on a host rusthinq has no business
//! ever passing through. `confirm_local` exists to make that mistake impossible to
//! repeat for a host proven to work locally, whatever `note`/`note_urls_in` or the
//! reactive path later think they saw — see the production incident documented on
//! `confirmed_local` below.

use rusthinq_util::sync::Mutex;
use std::collections::HashSet;

pub struct FirmwareHosts {
    // Deliberately starts empty. Naming a CDN here would contradict the point above —
    // right for one region, wrong elsewhere — and would also mask a wrong read of
    // startFota: with a host already present, an update succeeds whether or not it
    // was parsed correctly, so there'd be no way to tell.
    hosts: Mutex<HashSet<String>>,

    // Hosts rusthinq is known to answer itself — proven by a client actually
    // completing a TLS handshake against a certificate issued for it, not merely by a
    // certificate having been minted (an appliance can still reject that cert).
    //
    // Production incident this exists for: kic-common.lgthinq.com — the shared API
    // host practically every appliance and the real ThinQ app talks to — got one
    // rejected handshake (an unrelated caller, not pinned to rusthinq's CA) that the
    // reactive `note` path misread as "wants a real root", and passed the whole host
    // through from then on. That host is exactly what this set protects: it must
    // never be added to `hosts` again, no matter what evidence turns up later,
    // because a false positive here is not one broken download — it's every
    // appliance's control breaking at once.
    confirmed_local: Mutex<HashSet<String>>,
}

impl Default for FirmwareHosts {
    fn default() -> Self {
        Self::new()
    }
}

impl FirmwareHosts {
    pub fn new() -> Self {
        Self {
            hosts: Mutex::new(HashSet::new()),
            confirmed_local: Mutex::new(HashSet::new()),
        }
    }

    /// Record that a client has completed a real TLS handshake against a certificate
    /// issued for `host` — proof it's genuinely served locally, immune to `note`/
    /// `note_urls_in` for this host from now on, and evicted from `hosts` if a
    /// (mis)reactive `note` already added it.
    pub fn confirm_local(&self, host: &str) {
        self.confirmed_local.lock().insert(host.to_string());
        self.hosts.lock().remove(host);
    }

    /// Register the host of a firmware/download URL the cloud has just handed an
    /// appliance. Anything that isn't a parseable http(s) URL is ignored, and so is a
    /// host already proven to work when rusthinq answers it directly.
    pub fn note(&self, download_url: &str) {
        let Ok(parsed) = url::Url::parse(download_url) else {
            return;
        };
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return;
        }
        let Some(host) = parsed.host_str() else {
            return;
        };

        if self.confirmed_local.lock().contains(host) {
            rusthinq_core::logging::log(
                "status",
                &[&format!(
                    "refusing to pass {host} through - rusthinq has already answered it directly"
                )],
            );
            return;
        }

        let mut hosts = self.hosts.lock();
        rusthinq_core::logging::log(
            "status",
            &[&format!(
                "firmware download announced for {host}{}",
                if hosts.contains(host) {
                    ""
                } else {
                    " (new host)"
                }
            )],
        );
        hosts.insert(host.to_string());
    }

    /// Walk an arbitrary cloud→device payload and `note` every http(s) URL found in
    /// it, at any depth and under any field name — there is no fixed cmd/field that
    /// carries one (`startFota` is the original one, SOTA app content is another, and
    /// likely not the last), so any string that parses as an http(s) URL anywhere in
    /// the payload is treated as one.
    /// Only ever called from main.rs's `bridge` feature-gated wiring (a downlink CLIP
    /// stream only exists in bridge mode) — the reactive `note` path above still works
    /// without it.
    #[allow(dead_code)]
    pub fn note_urls_in(&self, payload: &serde_json::Value) {
        match payload {
            serde_json::Value::String(s) => self.note(s),
            serde_json::Value::Array(items) => {
                for item in items {
                    self.note_urls_in(item);
                }
            }
            serde_json::Value::Object(map) => {
                for value in map.values() {
                    self.note_urls_in(value);
                }
            }
            _ => {}
        }
    }

    /// Whether a TLS server name belongs to a firmware/SOTA download, and so should
    /// be handed to the real server rather than answered here.
    pub fn has(&self, name: &str) -> bool {
        self.hosts.lock().contains(name)
    }

    /// Exposed for tests/diagnostics.
    #[allow(dead_code)]
    pub fn all(&self) -> Vec<String> {
        self.hosts.lock().iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REAL_URL: &str = "https://objectcontent.lgthinq.com/fw/some-image.bin";

    #[test]
    fn note_registers_the_host_of_an_https_download_url() {
        let hosts = FirmwareHosts::new();
        hosts.note(REAL_URL);
        assert!(hosts.has("objectcontent.lgthinq.com"));
    }

    #[test]
    fn note_ignores_non_url_and_non_http_strings() {
        let hosts = FirmwareHosts::new();
        hosts.note("not a url");
        hosts.note("ftp://example.com/x");
        hosts.note("");
        assert_eq!(hosts.all(), Vec::<String>::new());
    }

    #[test]
    fn note_urls_in_finds_a_url_at_any_depth_under_any_field_name() {
        let hosts = FirmwareHosts::new();
        let payload = serde_json::json!({
            "cmd": "osp_command",
            "data": {
                "nested": { "whatever_field_name": REAL_URL },
                "list": [1, "not a url", { "another": "https://second.lgthinq.com/x" }],
            },
        });
        hosts.note_urls_in(&payload);
        assert!(hosts.has("objectcontent.lgthinq.com"));
        assert!(hosts.has("second.lgthinq.com"));
    }

    #[test]
    fn note_urls_in_does_not_loop_forever_on_a_deeply_nested_payload() {
        let hosts = FirmwareHosts::new();
        let mut payload = serde_json::json!(REAL_URL);
        for _ in 0..64 {
            payload = serde_json::json!({ "inner": payload });
        }
        hosts.note_urls_in(&payload);
        assert!(hosts.has("objectcontent.lgthinq.com"));
    }

    #[test]
    fn confirm_local_makes_a_host_immune_to_note_even_for_a_url_naming_it_directly() {
        let hosts = FirmwareHosts::new();
        hosts.confirm_local("kic-common.lgthinq.com");
        hosts.note("https://kic-common.lgthinq.com/some/path");

        assert!(!hosts.has("kic-common.lgthinq.com"));
        assert_eq!(hosts.all(), Vec::<String>::new());
    }

    #[test]
    fn confirm_local_makes_a_host_immune_to_note_urls_in_too() {
        let hosts = FirmwareHosts::new();
        hosts.confirm_local("kic-common.lgthinq.com");
        hosts.note_urls_in(&serde_json::json!({
            "cmd": "whatever",
            "data": { "url": "https://kic-common.lgthinq.com/x" },
        }));
        assert!(!hosts.has("kic-common.lgthinq.com"));
    }

    /// The production incident this whole file exists for: a caller that was never
    /// going to trust rusthinq's CA rejects the handshake, and the reactive
    /// tlsClientError path (see sni_passthrough.rs's caller in main.rs) misreads that
    /// as a firmware host — exactly what happened to kic-common.lgthinq.com. A later
    /// real request from a client that does trust it (proof the host is genuinely
    /// served locally) must undo that, not just prevent new ones.
    #[test]
    fn confirm_local_evicts_a_host_note_already_misadded() {
        let hosts = FirmwareHosts::new();
        hosts.note("https://kic-common.lgthinq.com/");
        assert!(hosts.has("kic-common.lgthinq.com"));

        hosts.confirm_local("kic-common.lgthinq.com");
        assert!(!hosts.has("kic-common.lgthinq.com"));
    }

    #[test]
    fn confirm_local_does_not_affect_other_hosts() {
        let hosts = FirmwareHosts::new();
        hosts.note(REAL_URL);
        hosts.confirm_local("kic-common.lgthinq.com");
        assert!(hosts.has("objectcontent.lgthinq.com"));
    }

    #[test]
    fn two_registries_do_not_share_state() {
        let a = FirmwareHosts::new();
        let b = FirmwareHosts::new();
        a.note(REAL_URL);
        assert!(a.has("objectcontent.lgthinq.com"));
        assert!(!b.has("objectcontent.lgthinq.com"));
    }
}
