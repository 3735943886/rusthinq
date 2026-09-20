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
//!
//! The two ways a host lands in `hosts` are not equally trustworthy, and are no longer
//! treated as if they were. `note_urls_in` reads an actual `startFota`/SOTA URL out of
//! a real cloud→device command — the appliance was just told to fetch this exact host,
//! so there is nothing to guess. The reactive path (`note`, called from main.rs's TLS
//! accept failure handlers) has no such evidence: a TLS handshake failing on this port
//! looks *identical* whether the caller genuinely wanted a firmware CDN or was simply
//! never going to trust rusthinq's CA for an unrelated reason (see the kic-common.lgthinq.com
//! incident above) — including, as a second production incident confirmed, an
//! appliance's own otherwise-healthy TLS stack occasionally failing a handshake it
//! would have passed on a retry a few seconds later, if passthrough hadn't stood in the
//! way of that retry ever reaching local termination again. A command-confirmed host is
//! marked permanently, same as before; a merely-suspected one expires (see
//! `INITIAL_SUSPECTED_TTL`/`RENEWED_SUSPECTED_TTL`) and reverts to attempting local
//! termination, giving a wrongly-noted host — or an appliance's own transient
//! glitch — a way back instead of being wrong
//! forever.

use rusthinq_util::sync::Mutex;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// How long a *reactively-suspected* host stays routed away from local termination
/// after the single failure that put it there, with no corroborating evidence yet.
/// This is the weakest possible signal — see the module doc — so it gets the
/// shortest leash: the two production incidents this module exists for both
/// self-corrected in single-digit seconds once a real retry reached local
/// termination, so 60s is already a wide margin over that observed recovery time,
/// not the 300s originally guessed at with no data behind it.
const INITIAL_SUSPECTED_TTL: Duration = Duration::from_secs(60);

/// How long a suspected host stays routed away from local termination after `has`
/// actually sees it *used* — i.e. some connection genuinely needed the passthrough,
/// which is real corroborating evidence this is a firmware/SOTA host and not the
/// single-failure fluke `INITIAL_SUSPECTED_TTL` has to assume the worst about. Longer
/// so a real download's connections, however far apart, don't wrongly get bounced to
/// local termination mid-transfer just because two of them happened to be more than a
/// minute apart.
const RENEWED_SUSPECTED_TTL: Duration = Duration::from_secs(600);

#[derive(Debug)]
enum Confidence {
    /// From `note_urls_in`: an actual `startFota`/SOTA command named this host.
    Confirmed,
    /// From the reactive `note`, or a live `has` renewal — inferred, not commanded,
    /// so it expires. Carries its own TTL (`INITIAL_SUSPECTED_TTL` fresh from `note`,
    /// `RENEWED_SUSPECTED_TTL` once `has` has seen it actually used) rather than a
    /// single constant, since how long this should last depends on how it got here.
    Suspected(Instant, Duration),
}

pub struct FirmwareHosts {
    // Deliberately starts empty. Naming a CDN here would contradict the point above —
    // right for one region, wrong elsewhere — and would also mask a wrong read of
    // startFota: with a host already present, an update succeeds whether or not it
    // was parsed correctly, so there'd be no way to tell.
    hosts: Mutex<HashMap<String, Confidence>>,

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
            hosts: Mutex::new(HashMap::new()),
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

    /// Register the host of a firmware/download URL *inferred* from a TLS handshake
    /// failure alone — see the module doc on why that inference is unreliable. Marks
    /// the host suspected for `INITIAL_SUSPECTED_TTL` (short: this one failure is not
    /// yet corroborated by anything — see `has` for what happens once it is), not
    /// permanently: a host already command-confirmed is left alone rather than
    /// downgraded. Anything that isn't a parseable http(s) URL is ignored, and so is a
    /// host already proven to work when rusthinq answers it directly.
    pub fn note(&self, download_url: &str) {
        let Some(host) = Self::parse_host(download_url) else {
            return;
        };

        if self.confirmed_local.lock().contains(&host) {
            rusthinq_core::logging::log(
                "status",
                &[&format!(
                    "refusing to pass {host} through - rusthinq has already answered it directly"
                )],
            );
            return;
        }

        let mut hosts = self.hosts.lock();
        let is_new = !hosts.contains_key(&host);
        // A command-confirmed host is not weakened by a later TLS-failure guess.
        if matches!(hosts.get(&host), Some(Confidence::Confirmed)) {
            return;
        }
        rusthinq_core::logging::log(
            "status",
            &[&format!(
                "firmware download suspected for {host}{}",
                if is_new { " (new host)" } else { "" }
            )],
        );
        hosts.insert(
            host,
            Confidence::Suspected(Instant::now(), INITIAL_SUSPECTED_TTL),
        );
    }

    /// Register the host of a firmware/download URL the cloud just handed an appliance
    /// in an actual command (`startFota` or equivalent) — real evidence, not a guess.
    /// Marks the host permanently, same as the old single-tier `note` used to.
    pub fn note_confirmed(&self, download_url: &str) {
        let Some(host) = Self::parse_host(download_url) else {
            return;
        };

        if self.confirmed_local.lock().contains(&host) {
            rusthinq_core::logging::log(
                "status",
                &[&format!(
                    "refusing to pass {host} through - rusthinq has already answered it directly"
                )],
            );
            return;
        }

        let mut hosts = self.hosts.lock();
        let is_new = !hosts.contains_key(&host);
        rusthinq_core::logging::log(
            "status",
            &[&format!(
                "firmware download announced for {host}{}",
                if is_new { " (new host)" } else { "" }
            )],
        );
        hosts.insert(host, Confidence::Confirmed);
    }

    /// Walk a device's own self-reported deploy metadata (`api-server`,
    /// `https-server`, `mqtt-server`, or any future field shaped like it — `ssl://`
    /// included, unlike `note`/`note_urls_in`, which only handle http(s)) and
    /// `confirm_local` every host named in it. A device that just finished a full
    /// local CLIP provisioning handshake has, by definition, already validated
    /// rusthinq's certificate for its own connection; the hosts it separately reports
    /// here are the core cloud endpoints it expects to keep reaching directly, never a
    /// firmware CDN — there's no confidence tier to weigh the way there is for a
    /// command-issued download URL, so this goes straight to confirmed, immune to
    /// `note` for good, the same host a lucky first HTTPS hit would have earned.
    pub fn confirm_local_urls_in(&self, payload: &serde_json::Value) {
        match payload {
            serde_json::Value::String(s) => {
                if let Ok(parsed) = url::Url::parse(s)
                    && let Some(host) = parsed.host_str()
                {
                    self.confirm_local(host);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    self.confirm_local_urls_in(item);
                }
            }
            serde_json::Value::Object(map) => {
                for value in map.values() {
                    self.confirm_local_urls_in(value);
                }
            }
            _ => {}
        }
    }

    fn parse_host(download_url: &str) -> Option<String> {
        let parsed = url::Url::parse(download_url).ok()?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            return None;
        }
        Some(parsed.host_str()?.to_string())
    }

    /// Walk an arbitrary cloud→device payload and `note_confirmed` every http(s) URL
    /// found in it, at any depth and under any field name — there is no fixed
    /// cmd/field that carries one (`startFota` is the original one, SOTA app content
    /// is another, and likely not the last), so any string that parses as an http(s)
    /// URL anywhere in the payload is treated as one. This is the command-confirmed
    /// path: the payload itself is the real cloud→device message naming the host.
    /// Only ever called from main.rs's `bridge` feature-gated wiring (a downlink CLIP
    /// stream only exists in bridge mode) — the reactive `note` path above still works
    /// without it.
    #[allow(dead_code)]
    pub fn note_urls_in(&self, payload: &serde_json::Value) {
        match payload {
            serde_json::Value::String(s) => self.note_confirmed(s),
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
    /// be handed to the real server rather than answered here. A merely-suspected
    /// entry stops counting once its own TTL has passed, letting the next connection
    /// attempt local termination again.
    pub fn has(&self, name: &str) -> bool {
        let mut hosts = self.hosts.lock();
        match hosts.get(name) {
            Some(Confidence::Confirmed) => true,
            Some(Confidence::Suspected(since, ttl)) => {
                if since.elapsed() >= *ttl {
                    return false;
                }
                // Sliding window, and upgraded: a host actually being *used* for
                // passthrough right now is no longer just the single unconfirmed
                // failure `note` recorded — something genuinely needed it, real
                // corroborating evidence — so the renewed window is the longer
                // `RENEWED_SUSPECTED_TTL`, not just a repeat of the short initial one.
                // A real download's connections, however long the whole thing takes,
                // keep it alive for as long as it actually runs; only a genuine gap
                // in use lets it expire.
                hosts.insert(
                    name.to_string(),
                    Confidence::Suspected(Instant::now(), RENEWED_SUSPECTED_TTL),
                );
                true
            }
            None => false,
        }
    }

    /// Exposed for tests/diagnostics. Includes expired-but-not-yet-evicted suspected
    /// entries; use `has` to check whether a name is currently routed away.
    #[allow(dead_code)]
    pub fn all(&self) -> Vec<String> {
        self.hosts.lock().keys().cloned().collect()
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

    /// A merely-suspected host (reactive `note`, no command evidence) must not stay
    /// routed away forever — the whole point of splitting confidence tiers. Backdates
    /// the clock directly rather than sleeping a real TTL in a test.
    #[test]
    fn a_suspected_host_stops_being_routed_away_once_the_ttl_has_passed() {
        let hosts = FirmwareHosts::new();

        let long_ago = Instant::now()
            .checked_sub(INITIAL_SUSPECTED_TTL + Duration::from_secs(1))
            .expect("test clock underflow");
        hosts.hosts.lock().insert(
            "objectcontent.lgthinq.com".into(),
            Confidence::Suspected(long_ago, INITIAL_SUSPECTED_TTL),
        );

        assert!(!hosts.has("objectcontent.lgthinq.com"));
    }

    /// A command-confirmed host (`note_urls_in`, real `startFota`-style evidence) has
    /// no TTL at all — unlike a suspected one, it must stay immune indefinitely.
    /// `Confidence::Confirmed` carries no timestamp at all, so unlike the suspected
    /// case there is nothing to backdate here — this just checks it was stored as
    /// `Confirmed` rather than `Suspected` in the first place.
    #[test]
    fn a_confirmed_host_does_not_expire() {
        let hosts = FirmwareHosts::new();
        hosts.note_confirmed(REAL_URL);

        assert!(matches!(
            hosts.hosts.lock().get("objectcontent.lgthinq.com"),
            Some(Confidence::Confirmed)
        ));
        assert!(hosts.has("objectcontent.lgthinq.com"));
    }

    /// A reactive guess must never weaken a host the cloud already confirmed via a
    /// real command — otherwise a single unrelated TLS failure could downgrade a
    /// permanently-safe classification back down to something that expires.
    #[test]
    fn note_does_not_downgrade_an_already_confirmed_host() {
        let hosts = FirmwareHosts::new();
        hosts.note_confirmed(REAL_URL);
        hosts.note(REAL_URL);

        assert!(matches!(
            hosts.hosts.lock().get("objectcontent.lgthinq.com"),
            Some(Confidence::Confirmed)
        ));
    }

    /// The fix for the actual incident this whole investigation was about: a device's
    /// own deploy metadata (api-server/https-server/mqtt-server, ssl:// included) gets
    /// immunized the moment it finishes local provisioning — not only after a lucky
    /// first HTTPS hit on that exact hostname from some other caller.
    #[test]
    fn confirm_local_urls_in_immunizes_a_devices_self_reported_endpoints() {
        let hosts = FirmwareHosts::new();
        hosts.confirm_local_urls_in(&serde_json::json!({
            "api-server": "https://kic-common.lgthinq.com:443",
            "mqtt-server": "ssl://common.iot.kic.lgthinq.com:8883",
            "https-server": "https://kic-mclip.lgthinq.com:443",
        }));

        // Immune the same way an HTTPS-port confirm_local would have made it, so a
        // later reactive note() for any of them is a no-op.
        hosts.note("https://kic-common.lgthinq.com/some/path");
        assert!(!hosts.has("kic-common.lgthinq.com"));
        assert!(!hosts.has("common.iot.kic.lgthinq.com"));
        assert!(!hosts.has("kic-mclip.lgthinq.com"));
    }

    /// A suspected host actually being used — `has` called again while it's still
    /// live, the same as a real download's next connection would — renews its own
    /// window (to the longer `RENEWED_SUSPECTED_TTL`) instead of expiring on the
    /// original `note`'s short initial clock. A download that runs longer than
    /// `INITIAL_SUSPECTED_TTL` in total must not have its later connections wrongly
    /// bounced to local termination just because the *first* one is old.
    #[test]
    fn a_live_hit_renews_the_suspected_window_instead_of_expiring_on_schedule() {
        let hosts = FirmwareHosts::new();

        // Backdate it to just inside the *initial* window — old enough that it would
        // fail on the next check without a renewal, not so old this `has` call
        // itself reads as already-expired.
        let almost_expired = Instant::now()
            .checked_sub(INITIAL_SUSPECTED_TTL - Duration::from_millis(50))
            .expect("test clock underflow");
        hosts.hosts.lock().insert(
            "objectcontent.lgthinq.com".into(),
            Confidence::Suspected(almost_expired, INITIAL_SUSPECTED_TTL),
        );

        // This `has` call is the renewal: it must see the host as still suspected,
        // and upgrade it to the longer RENEWED_SUSPECTED_TTL — not just repeat the
        // short initial one — since a live hit is real corroborating evidence.
        assert!(hosts.has("objectcontent.lgthinq.com"));

        match hosts.hosts.lock().get("objectcontent.lgthinq.com") {
            Some(Confidence::Suspected(since, ttl)) => {
                assert!(
                    *since > almost_expired,
                    "has() must have pushed the timestamp forward, not left the old one in place"
                );
                assert_eq!(
                    *ttl, RENEWED_SUSPECTED_TTL,
                    "a live hit must upgrade to the longer renewed TTL, not the short initial one"
                );
            }
            other => panic!("expected a renewed Suspected entry, got {other:?}"),
        }
    }
}
