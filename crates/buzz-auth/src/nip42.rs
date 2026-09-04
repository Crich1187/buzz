//! NIP-42 challenge/response authentication.
//!
//! 1. Relay sends `["AUTH", "<challenge>"]` via [`generate_challenge`].
//! 2. Client signs a kind:22242 event with challenge + relay tags.
//! 3. Relay validates via [`verify_nip42_event`].
//!
//! AUTH events are **never** stored or logged (may contain bearer tokens).

use nostr::{Event, Kind, TagKind, Timestamp};
use url::Url;

use crate::error::AuthError;

/// Normalize a relay URL for comparison.
///
/// Uses the `url` crate for proper parsing rather than string manipulation.
/// Normalizes localhost variants to 127.0.0.1 and strips trailing slashes
/// (the `url` crate handles the latter automatically via path normalization).
fn normalize_relay_url(raw: &str) -> String {
    let mut parsed = match Url::parse(raw) {
        Ok(u) => u,
        Err(_) => return raw.to_string(),
    };
    // Treat localhost variants as equivalent by normalizing to 127.0.0.1.
    if let Some(host) = parsed.host_str() {
        if host == "localhost" || host == "::1" {
            let _ = parsed.set_host(Some("127.0.0.1"));
        }
    }
    let path = parsed.path().trim_end_matches('/').to_string();
    parsed.set_path(&path);
    parsed.to_string()
}

const TIMESTAMP_TOLERANCE_SECS: u64 = 60;

/// Generate a random NIP-42 challenge (32 CSPRNG bytes, hex-encoded).
pub fn generate_challenge() -> String {
    let bytes: [u8; 32] = rand::random();
    hex::encode(bytes)
}

/// Verify a NIP-42 AUTH event against a single accepted relay URL.
///
/// Checks kind, signature, challenge, relay URL, and timestamp (±60s).
/// CPU-bound (Schnorr verify) — call via `spawn_blocking` in async contexts.
///
/// Deployments that terminate TLS in front of the relay and also accept an
/// internal plaintext transport must use [`verify_nip42_event_against`] with an
/// explicitly configured accepted set rather than loosening this comparison.
pub fn verify_nip42_event(
    event: &Event,
    expected_challenge: &str,
    relay_url: &str,
) -> Result<(), AuthError> {
    verify_nip42_event_against(event, expected_challenge, std::slice::from_ref(&relay_url))
}

/// Verify a NIP-42 AUTH event against a bounded set of accepted relay URLs.
///
/// # Why a set, and why this does not weaken authentication
///
/// The relay tag exists to bind an AUTH event to *this* relay so a signature
/// captured elsewhere cannot be replayed here. That binding is preserved: the
/// tag must still match an entry the operator configured, exactly, after
/// normalization. What the set adds is the ability to name the same relay under
/// more than one transport — a public `wss://` identity and an internal
/// plaintext `ws://` hop through a loopback terminator are the same relay, and
/// the caller is responsible for constructing the set so that every entry
/// shares the connection's resolved host (see the relay's
/// `nip42_accepted_relay_urls`). An empty or single-entry set behaves exactly
/// as the historical single-URL check did.
pub fn verify_nip42_event_against<S: AsRef<str>>(
    event: &Event,
    expected_challenge: &str,
    accepted_relay_urls: &[S],
) -> Result<(), AuthError> {
    if accepted_relay_urls.is_empty() {
        // Fail closed: an empty accepted set can never be satisfied, and
        // silently passing here would remove the relay binding entirely.
        return Err(AuthError::RelayUrlMismatch);
    }
    if event.kind != Kind::Authentication {
        return Err(AuthError::InvalidSignature);
    }

    buzz_core::verify_event(event).map_err(|_| AuthError::InvalidSignature)?;

    let challenge = event
        .tags
        .find(TagKind::Challenge)
        .and_then(|t| t.content())
        .ok_or(AuthError::ChallengeMismatch)?;

    if challenge != expected_challenge {
        return Err(AuthError::ChallengeMismatch);
    }

    let relay = event
        .tags
        .find(TagKind::Relay)
        .and_then(|t| t.content())
        .ok_or(AuthError::RelayUrlMismatch)?;

    // root-jk1sw Gate4 Blocker: the relay tag must match one of the *explicitly
    // accepted* identities for this connection. `accepted_relay_urls` is built
    // by the caller from the canonical URL plus any configured transport
    // aliases; an empty extra set degenerates to the single-URL behaviour this
    // function has always had. Matching is still exact-after-normalization —
    // nothing here relaxes scheme, host, or port on its own.
    let normalized = normalize_relay_url(relay);
    if !accepted_relay_urls
        .iter()
        .any(|accepted| normalize_relay_url(accepted.as_ref()) == normalized)
    {
        return Err(AuthError::RelayUrlMismatch);
    }

    let now = Timestamp::now().as_secs();
    let event_ts = event.created_at.as_secs();
    let delta = now.abs_diff(event_ts);
    if delta > TIMESTAMP_TOLERANCE_SECS {
        return Err(AuthError::EventExpired);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind, RelayUrl, Timestamp};

    const TEST_RELAY: &str = "wss://relay.example.com";

    fn make_auth_event(keys: &Keys, challenge: &str, relay_url: &str) -> Event {
        let url = RelayUrl::parse(relay_url).expect("valid relay url");
        EventBuilder::auth(challenge, url)
            .sign_with_keys(keys)
            .expect("signing failed")
    }

    #[test]
    fn challenge_is_64_hex_chars_and_unique() {
        let c1 = generate_challenge();
        let c2 = generate_challenge();
        assert_eq!(c1.len(), 64);
        assert!(c1.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(c1, c2);
    }

    #[test]
    fn valid_event_passes() {
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, TEST_RELAY);
        assert!(verify_nip42_event(&event, &challenge, TEST_RELAY).is_ok());
    }

    #[test]
    fn wrong_challenge_rejected() {
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, TEST_RELAY);
        assert!(matches!(
            verify_nip42_event(&event, "wrong", TEST_RELAY),
            Err(AuthError::ChallengeMismatch)
        ));
    }

    #[test]
    fn wrong_kind_rejected() {
        let keys = Keys::generate();
        let event = EventBuilder::new(Kind::TextNote, "not auth")
            .tags([])
            .sign_with_keys(&keys)
            .expect("sign");
        assert!(matches!(
            verify_nip42_event(&event, "x", TEST_RELAY),
            Err(AuthError::InvalidSignature)
        ));
    }

    #[test]
    fn expired_event_rejected() {
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let url = RelayUrl::parse(TEST_RELAY).unwrap();
        let old_ts = Timestamp::from(Timestamp::now().as_secs().saturating_sub(120));
        let event = EventBuilder::auth(&challenge, url)
            .custom_created_at(old_ts)
            .sign_with_keys(&keys)
            .expect("sign");
        assert!(matches!(
            verify_nip42_event(&event, &challenge, TEST_RELAY),
            Err(AuthError::EventExpired)
        ));
    }

    #[test]
    fn wrong_relay_rejected() {
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, "wss://other.example.com");
        assert!(matches!(
            verify_nip42_event(&event, &challenge, TEST_RELAY),
            Err(AuthError::RelayUrlMismatch)
        ));
    }

    // ---------------------------------------------------------------------
    // root-jk1sw Gate4 Blocker — bounded transport aliasing.
    //
    // The deployed relay refused every fleet connection because the fleet
    // signs the internal plaintext transport (`ws://`) while the relay only
    // accepted its canonical public identity (`wss://`). These tests pin the
    // exact contract of the fix: an alias is honoured only when the operator
    // listed it, and nothing else about the relay binding is relaxed.
    // ---------------------------------------------------------------------

    const TEST_RELAY_PLAINTEXT: &str = "ws://relay.example.com";

    #[test]
    fn configured_plaintext_alias_is_accepted_for_the_same_relay() {
        let keys = Keys::generate();
        let challenge = generate_challenge();
        // Client signs the internal plaintext transport, exactly as the
        // buzz-acp fleet does through the loopback terminator.
        let event = make_auth_event(&keys, &challenge, TEST_RELAY_PLAINTEXT);
        let accepted = [TEST_RELAY, TEST_RELAY_PLAINTEXT];
        assert!(verify_nip42_event_against(&event, &challenge, &accepted).is_ok());
    }

    #[test]
    fn canonical_identity_still_verifies_when_an_alias_is_configured() {
        // The alias must not displace the public identity: both work.
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, TEST_RELAY);
        let accepted = [TEST_RELAY, TEST_RELAY_PLAINTEXT];
        assert!(verify_nip42_event_against(&event, &challenge, &accepted).is_ok());
    }

    #[test]
    fn plaintext_alias_is_rejected_when_it_is_not_configured() {
        // Sabotage: drop the alias from the accepted set. The same event that
        // passes above must now fail — proving the acceptance came from the
        // configured entry and not from a loosened scheme comparison.
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, TEST_RELAY_PLAINTEXT);
        assert!(matches!(
            verify_nip42_event_against(&event, &challenge, &[TEST_RELAY]),
            Err(AuthError::RelayUrlMismatch)
        ));
    }

    #[test]
    fn alias_does_not_admit_a_foreign_host_under_either_scheme() {
        // The anti-replay property that matters: configuring a plaintext alias
        // for *this* relay must never accept an AUTH signed for another relay.
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let accepted = [TEST_RELAY, TEST_RELAY_PLAINTEXT];
        for foreign in ["wss://other.example.com", "ws://other.example.com"] {
            let event = make_auth_event(&keys, &challenge, foreign);
            assert!(
                matches!(
                    verify_nip42_event_against(&event, &challenge, &accepted),
                    Err(AuthError::RelayUrlMismatch)
                ),
                "foreign relay {foreign} must never be accepted"
            );
        }
    }

    #[test]
    fn empty_accepted_set_fails_closed() {
        // An empty set must never mean "accept anything".
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, TEST_RELAY);
        let empty: [&str; 0] = [];
        assert!(matches!(
            verify_nip42_event_against(&event, &challenge, &empty),
            Err(AuthError::RelayUrlMismatch)
        ));
    }

    #[test]
    fn aliasing_does_not_weaken_the_other_auth_checks() {
        // Challenge, kind, and freshness must still be enforced with an alias
        // set configured — the fix is scoped to the relay tag alone.
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let accepted = [TEST_RELAY, TEST_RELAY_PLAINTEXT];

        let event = make_auth_event(&keys, &challenge, TEST_RELAY_PLAINTEXT);
        assert!(matches!(
            verify_nip42_event_against(&event, "wrong-challenge", &accepted),
            Err(AuthError::ChallengeMismatch)
        ));

        let url = RelayUrl::parse(TEST_RELAY_PLAINTEXT).unwrap();
        let old_ts = Timestamp::from(Timestamp::now().as_secs().saturating_sub(120));
        let stale = EventBuilder::auth(&challenge, url)
            .custom_created_at(old_ts)
            .sign_with_keys(&keys)
            .expect("sign");
        assert!(matches!(
            verify_nip42_event_against(&stale, &challenge, &accepted),
            Err(AuthError::EventExpired)
        ));

        let wrong_kind = EventBuilder::new(Kind::TextNote, "not auth")
            .tags([])
            .sign_with_keys(&keys)
            .expect("sign");
        assert!(matches!(
            verify_nip42_event_against(&wrong_kind, &challenge, &accepted),
            Err(AuthError::InvalidSignature)
        ));
    }

    #[test]
    fn single_url_helper_matches_the_accepted_set_form() {
        // `verify_nip42_event` must remain exactly the one-entry case.
        let keys = Keys::generate();
        let challenge = generate_challenge();
        let event = make_auth_event(&keys, &challenge, TEST_RELAY);
        assert!(verify_nip42_event(&event, &challenge, TEST_RELAY).is_ok());
        assert!(verify_nip42_event_against(&event, &challenge, &[TEST_RELAY]).is_ok());
        assert!(verify_nip42_event(&event, &challenge, TEST_RELAY_PLAINTEXT).is_err());
    }

    #[test]
    fn localhost_and_127_are_equivalent() {
        let a = normalize_relay_url("ws://localhost:3030");
        let b = normalize_relay_url("ws://127.0.0.1:3030");
        assert_eq!(a, b);
    }

    #[test]
    fn trailing_slash_normalized() {
        let a = normalize_relay_url("wss://relay.example.com/");
        let b = normalize_relay_url("wss://relay.example.com");
        assert_eq!(a, b);
    }
}
