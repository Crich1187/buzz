//! Controlled-transport signer for root-ylp2d functional proof.
//!
//! Usage: `ylp2d_sign_marker <marker-text>`
//! Prints one JSON object: event_id, pubkey, has_marker (booleans/hex only).

use buzz_sdk::build_message;
use nostr::Keys;
use uuid::Uuid;

fn main() {
    let marker = std::env::args()
        .nth(1)
        .expect("usage: ylp2d_sign_marker <marker>");
    let keys = Keys::generate();
    let channel = Uuid::new_v4();
    let builder =
        build_message(channel, &marker, None, &[], false, &[], &[]).expect("build_message");
    let event = builder.sign_with_keys(&keys).expect("sign");
    let event_id = event.id.to_hex();
    let pubkey = keys.public_key().to_hex();
    let has_marker = event.content.contains(&marker);
    println!(
        "{}",
        serde_json::json!({
            "event_id": event_id,
            "pubkey": pubkey,
            "has_marker": has_marker,
            "content_len": event.content.len(),
        })
    );
}
