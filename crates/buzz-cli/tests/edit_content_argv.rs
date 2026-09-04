//! root-67m5i: prove argv-safe `messages edit` paths keep private bodies out
//! of `/proc/<pid>/cmdline`. Assertions are value-safe (booleans / lengths
//! only — never print message bodies, markers, or keys).

#![cfg(target_os = "linux")]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const EVENT: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const MARKER: &str = "PRIV67M5I_CMDLINE_ABSENT_MARKER";

fn cmdline_contains(raw: &[u8], needle: &[u8]) -> bool {
    raw.windows(needle.len()).any(|w| w == needle)
}

/// Ephemeral throwaway key so the CLI reaches content resolution (never logged).
fn throwaway_private_key_hex() -> String {
    nostr::Keys::generate().secret_key().to_secret_hex()
}

/// `--content -` with stdin held open: after a valid key is supplied the child
/// blocks in stdin read. Cmdline must never contain the private marker.
#[test]
fn edit_stdin_dash_omits_private_marker_from_proc_cmdline() {
    let buzz = env!("CARGO_BIN_EXE_buzz");
    let key = throwaway_private_key_hex();
    let mut child = Command::new(buzz)
        .args(["messages", "edit", "--event", EVENT, "--content", "-"])
        .env("BUZZ_RELAY_URL", "http://127.0.0.1:1")
        .env("BUZZ_PRIVATE_KEY", &key)
        .env_remove("BUZZ_NSEC")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn buzz messages edit --content -");

    let pid = child.id();
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut raw_seen: Option<Vec<u8>> = None;
    while Instant::now() < deadline {
        if let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) {
            if !raw.is_empty() && cmdline_contains(&raw, b"--content") {
                raw_seen = Some(raw);
                break;
            }
            if !raw.is_empty() {
                raw_seen = Some(raw);
            }
        }
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    let raw = raw_seen.expect("never observed /proc/<pid>/cmdline");
    let marker_in_argv = cmdline_contains(&raw, MARKER.as_bytes());
    let has_dash_content = cmdline_contains(&raw, b"--content");
    // Unblock: write marker (body never needed on argv), then close.
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(MARKER.as_bytes());
    }
    let _ = child.wait();

    assert!(has_dash_content, "expected --content in argv");
    assert!(
        !marker_in_argv,
        "private marker present in argv on stdin-safe path"
    );
}

/// `--content-file` with a mode-0600 body: marker lives only in the file.
/// Cmdline may include the path, never the marker bytes.
#[test]
fn edit_content_file_omits_private_marker_from_proc_cmdline() {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "buzz-edit-argv-{}-{}.body",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, MARKER).unwrap();
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(&path, perms).unwrap();

    let buzz = env!("CARGO_BIN_EXE_buzz");
    let key = throwaway_private_key_hex();
    let mut child = Command::new(buzz)
        .args([
            "messages",
            "edit",
            "--event",
            EVENT,
            "--content-file",
            path.to_str().unwrap(),
        ])
        .env("BUZZ_RELAY_URL", "http://127.0.0.1:1")
        .env("BUZZ_PRIVATE_KEY", &key)
        .env_remove("BUZZ_NSEC")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn buzz messages edit --content-file");

    let pid = child.id();
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut saw = false;
    let mut marker_in_argv = true;
    let mut has_flag = false;
    while Instant::now() < deadline {
        if let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) {
            if !raw.is_empty() {
                saw = true;
                marker_in_argv = cmdline_contains(&raw, MARKER.as_bytes());
                has_flag = cmdline_contains(&raw, b"--content-file");
                if has_flag {
                    break;
                }
            }
        }
        if let Ok(Some(_)) = child.try_wait() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);

    assert!(saw, "never observed /proc/<pid>/cmdline");
    assert!(has_flag, "expected --content-file in argv");
    assert!(
        !marker_in_argv,
        "private marker present in argv on content-file safe path"
    );
}
