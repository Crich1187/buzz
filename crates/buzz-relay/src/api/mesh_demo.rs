//! `POST /_mesh/demo/echo` — testbed-only join-side ingress for the mesh
//! reliable-stream smoke.
//!
//! This is the *client leg* of the `BUZZ_MESH_DEMO_ECHO` evidence run: the
//! owner-side echo consumer (see `mesh_boot::run_demo_echo`) validates and
//! echoes frames, but nothing in the product calls
//! [`ReliableStreamRouter::join`] yet — so cross-pod evidence needs a way to
//! drive a join from a chosen pod. This route is that way, and nothing more:
//!
//! - Gated on **both** `BUZZ_MESH_DEMO_ECHO=on` and mesh enabled; 404
//!   otherwise (the same strictness as the owner-side consumer — the route
//!   does not exist unless the operator opted the deployment into the demo).
//! - `Owned` result: this pod acquired the fenced lease. No renewer is
//!   spawned — this is a smoke, not a session — so the lease lives for its
//!   Redis TTL (30s default). Drive the owner pod first, then the forwarding
//!   pod within that window.
//! - `Forwarded` result: sends the payload to the owner over the mesh and
//!   waits (bounded) for the echoed frame, proving owner-side
//!   `recv_validated` (Redis fence included) and return-path delivery.
//!
//! Not a product flow. The real join-side consumer (goose/berd session
//! wiring) replaces this; the route stays demo-gated until it is deleted.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use buzz_core::CommunityId;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::state::AppState;
use crate::tunnel::directory::SessionDirectory;
use crate::tunnel::reliable::{
    ReliableFrame, ReliableJoin, ReliableStreamError, ReliableStreamRouter,
};
use buzz_relay_mesh::RelayPeerTransport;

/// How long the forwarded leg waits for the owner's echo before failing the
/// smoke. Generous relative to an intra-cluster RTT; small enough that a
/// wedged owner fails the run instead of hanging the probe.
const ECHO_TIMEOUT: Duration = Duration::from_secs(10);

/// Request body for the demo echo probe.
#[derive(Debug, Deserialize)]
pub struct DemoEchoRequest {
    /// Community/tenant scope for the fenced session.
    pub community_id: Uuid,
    /// Session to join. The first pod to post a given id becomes the owner.
    pub session_id: Uuid,
    /// Opaque payload for the echo round-trip (UTF-8 for readable evidence).
    pub payload: String,
}

/// `POST /_mesh/demo/echo` handler. 404 unless the deployment opted in.
pub async fn demo_echo(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DemoEchoRequest>,
) -> Response {
    // Same gate as the owner-side consumer: both flags, or the route does
    // not exist. 404 (not 403) so a non-demo deployment is indistinguishable
    // from one without the route.
    let Some(handle) = state.mesh() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !state.config.mesh_demo_echo {
        return StatusCode::NOT_FOUND.into_response();
    }

    let router = ReliableStreamRouter::new(
        handle.directory.clone(),
        Arc::clone(&handle.transport),
        handle.local_runtime_id,
    );
    run_demo_join(&router, &handle.directory, req).await
}

/// Core of the probe, split from the handler so tests can drive it with a
/// directory + transport pair without standing up an `AppState`.
async fn run_demo_join<T>(
    router: &ReliableStreamRouter<T>,
    directory: &SessionDirectory,
    req: DemoEchoRequest,
) -> Response
where
    T: RelayPeerTransport + ?Sized,
{
    let community_id = CommunityId::from_uuid(req.community_id);
    match router.join(community_id, req.session_id).await {
        // This pod took (or already holds) the fenced lease. The lease is
        // deliberately not renewed: it expires with its Redis TTL.
        Ok(ReliableJoin::Owned { lease }) => Json(json!({
            "outcome": "owned",
            "generation": lease.generation,
            "owner_runtime_id": lease.owner_runtime_id.to_string(),
        }))
        .into_response(),

        // Another pod owns the session: send the payload over the mesh and
        // wait for the owner-side echo consumer to bounce it back.
        Ok(ReliableJoin::Forwarded { lease, mut stream }) => {
            if let Err(e) = stream
                .send_bytes(community_id, req.payload.as_bytes())
                .await
            {
                return echo_error(StatusCode::BAD_GATEWAY, "send failed", &e);
            }
            let echoed = tokio::time::timeout(ECHO_TIMEOUT, stream.recv_validated(directory)).await;
            match echoed {
                Ok(Ok(Some(ReliableFrame::Data(bytes)))) => Json(json!({
                    "outcome": "forwarded",
                    "generation": lease.generation,
                    "owner_runtime_id": lease.owner_runtime_id.to_string(),
                    "echoed_payload": String::from_utf8_lossy(&bytes),
                }))
                .into_response(),
                Ok(Ok(Some(ReliableFrame::Goodbye(reason)))) => (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({"error": format!("owner sent goodbye: {reason:?}")})),
                )
                    .into_response(),
                Ok(Ok(None)) => (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({"error": "stream closed before echo"})),
                )
                    .into_response(),
                Ok(Err(e)) => echo_error(StatusCode::BAD_GATEWAY, "recv failed", &e),
                Err(_) => (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(json!({"error": "timed out waiting for echo"})),
                )
                    .into_response(),
            }
        }

        Err(e) => echo_error(StatusCode::BAD_GATEWAY, "join failed", &e),
    }
}

fn echo_error(status: StatusCode, what: &str, e: &ReliableStreamError) -> Response {
    (status, Json(json!({"error": format!("{what}: {e}")}))).into_response()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::to_bytes;
    use buzz_core::CommunityId;
    use buzz_relay_mesh::endpoint::MeshEndpoint;
    use buzz_relay_mesh::{
        InboundHandler, MeshDatagram, MeshError, MeshStream, MeshStreamFrame, Profile, RuntimeId,
        StreamHello,
    };
    use uuid::Uuid;

    use super::*;
    use crate::tunnel::directory::SessionDirectory;

    /// Logical Redis DB reserved for mesh-demo unit tests so shared DB 0
    /// (live relay / other suites) cannot poison join→forward fences.
    const MESH_DEMO_TEST_REDIS_DB: u16 = 15;

    /// Lease TTL for demo tests. Must exceed mesh bind/connect/accept under a
    /// busy parallel `cargo test` run; production default stays unchanged.
    const MESH_DEMO_TEST_LEASE_TTL: Duration = Duration::from_secs(60);

    /// Pin (or replace) the Redis DB index in a URL without printing secrets.
    fn pin_redis_db(url: &str, db: u16) -> String {
        let url = url.trim().trim_end_matches('/');
        if let Some(idx) = url.rfind('/') {
            let after = &url[idx + 1..];
            if !after.is_empty() && after.chars().all(|c| c.is_ascii_digit()) {
                return format!("{}{db}", &url[..=idx]);
            }
        }
        format!("{url}/{db}")
    }

    /// Controlled Redis URL for mesh-demo tests.
    ///
    /// Prefer `BUZZ_MESH_DEMO_TEST_REDIS_URL`. Otherwise take host/auth from
    /// `REDIS_URL` (or localhost) and force logical DB 15 so leftover keys on
    /// shared DB 0 cannot affect fences. Never logs the URL (may contain auth).
    fn mesh_demo_test_redis_url() -> String {
        if let Ok(url) = std::env::var("BUZZ_MESH_DEMO_TEST_REDIS_URL") {
            let trimmed = url.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        let base =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        pin_redis_db(&base, MESH_DEMO_TEST_REDIS_DB)
    }

    fn pool_for_url(url: &str) -> Option<deadpool_redis::Pool> {
        deadpool_redis::Config::from_url(url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .ok()
    }

    async fn redis_directory_for_mesh_demo() -> Option<SessionDirectory> {
        let url = mesh_demo_test_redis_url();
        let Some(pool) = pool_for_url(&url) else {
            eprintln!(
                "mesh_demo test skip: redis pool create failed (db={MESH_DEMO_TEST_REDIS_DB})"
            );
            return None;
        };
        let mut conn = match pool.get().await {
            Ok(conn) => conn,
            Err(err) => {
                eprintln!(
                    "mesh_demo test skip: redis unavailable (db={MESH_DEMO_TEST_REDIS_DB}): {err}"
                );
                return None;
            }
        };
        match redis::cmd("PING").query_async::<String>(&mut *conn).await {
            Ok(pong) if pong == "PONG" => {}
            Ok(_) | Err(_) => {
                eprintln!("mesh_demo test skip: redis PING failed (db={MESH_DEMO_TEST_REDIS_DB})");
                return None;
            }
        }
        Some(SessionDirectory::with_lease_ttl(
            pool,
            MESH_DEMO_TEST_LEASE_TTL,
        ))
    }

    /// Shared DB 0 pool — only for isolation-regression pollution writes.
    async fn shared_db0_pool() -> Option<deadpool_redis::Pool> {
        let base =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let url = pin_redis_db(&base, 0);
        let pool = pool_for_url(&url)?;
        let mut conn = pool.get().await.ok()?;
        redis::cmd("PING")
            .query_async::<String>(&mut *conn)
            .await
            .ok()?;
        Some(pool)
    }

    struct NoopTransport;

    impl RelayPeerTransport for NoopTransport {
        fn send_datagram(&self, _to: RuntimeId, _dgram: MeshDatagram) -> Result<(), MeshError> {
            unreachable!("demo owned-arm test never sends datagrams")
        }

        fn open_session_stream(
            &self,
            _to: RuntimeId,
            _hello: StreamHello,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<MeshStream, MeshError>> + Send + '_>,
        > {
            Box::pin(async { Err(MeshError::Transport("unexpected open".into())) })
        }

        fn set_inbound(&self, _handler: Box<dyn InboundHandler>) {}
    }

    struct DirectTransport {
        peer: buzz_relay_mesh::peer::MeshPeer,
    }

    impl RelayPeerTransport for DirectTransport {
        fn send_datagram(&self, _to: RuntimeId, _dgram: MeshDatagram) -> Result<(), MeshError> {
            unreachable!("demo forwarded-arm test never sends datagrams")
        }

        fn open_session_stream(
            &self,
            _to: RuntimeId,
            hello: StreamHello,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<MeshStream, MeshError>> + Send + '_>,
        > {
            Box::pin(async move {
                let mut stream = self.peer.open_bi().await?;
                stream.send_frame(MeshStreamFrame::Hello(hello)).await?;
                Ok(stream)
            })
        }

        fn set_inbound(&self, _handler: Box<dyn InboundHandler>) {}
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Drive owner→forwarder echo with the peer-pair order proven green by
    /// `tunnel::reliable::tests::later_join_routes_to_remote_owner_*`:
    /// accept/connect first, then arm the lease, then accept_bi + echo.
    ///
    /// The prior mesh_demo harness spawned accept+accept_bi together and raced
    /// the forwarder open_bi; on this host that path deterministically 504s
    /// (echo timeout) even on a clean Redis DB — independent of shared-state
    /// pollution, which this fixture also isolates via DB 15.
    async fn forwarded_echo_round_trip(
        directory: SessionDirectory,
        community_id: Uuid,
        session_id: Uuid,
        payload: &str,
    ) -> (StatusCode, serde_json::Value) {
        let community = CommunityId::from_uuid(community_id);
        directory
            .clear_session_keys(community, session_id)
            .await
            .expect("clear session keys on isolated redis");

        let bind = || "127.0.0.1:0".parse().unwrap();
        let local_endpoint = MeshEndpoint::bind(bind()).await.unwrap();
        let owner_endpoint = MeshEndpoint::bind(bind()).await.unwrap();
        let owner_runtime = owner_endpoint.runtime_id();
        let owner_addr = owner_endpoint.addr();

        let accept_endpoint = owner_endpoint.clone();
        let accept = tokio::spawn(async move { accept_endpoint.accept().await.unwrap().unwrap() });
        let local_peer = local_endpoint.connect(owner_addr).await.unwrap();
        let owner_peer = accept.await.unwrap();

        // Owner acquires the fenced lease first (Mari's podB-first order).
        let owner_router = ReliableStreamRouter::new(
            directory.clone(),
            std::sync::Arc::new(NoopTransport),
            owner_runtime,
        );
        let owned = run_demo_join(
            &owner_router,
            &directory,
            DemoEchoRequest {
                community_id,
                session_id,
                payload: "unused".into(),
            },
        )
        .await;
        assert_eq!(owned.status(), StatusCode::OK);
        assert_eq!(body_json(owned).await["outcome"], "owned");

        let echo_directory = directory.clone();
        let (echo_ready_tx, echo_ready_rx) = tokio::sync::oneshot::channel::<()>();
        let owner_task = tokio::spawn(async move {
            let mut stream = owner_peer.accept_bi().await.unwrap();
            let hello = match stream.recv_frame().await.unwrap().unwrap() {
                MeshStreamFrame::Hello(h) => h,
                other => panic!("expected hello, got {other:?}"),
            };
            let router = ReliableStreamRouter::new(
                echo_directory.clone(),
                std::sync::Arc::new(NoopTransport),
                owner_runtime,
            );
            let from = hello.sender;
            let inbound = router
                .accept_inbound(from, hello, stream)
                .await
                .expect("owner accept_inbound");
            let _ = echo_ready_tx.send(());
            crate::mesh_boot::run_demo_echo(
                echo_directory,
                inbound,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .await;
        });

        // Yield so the owner accept_bi task is polled before open_bi; without
        // this, the forwarder can open+send into a stream whose echo consumer
        // is not yet in recv_validated, and the 10s probe times out (504).
        tokio::task::yield_now().await;

        let local_router = ReliableStreamRouter::new(
            directory.clone(),
            std::sync::Arc::new(DirectTransport { peer: local_peer }),
            local_endpoint.runtime_id(),
        );
        let join = local_router
            .join(CommunityId::from_uuid(community_id), session_id)
            .await
            .expect("forwarder join");
        let crate::tunnel::reliable::ReliableJoin::Forwarded { lease, mut stream } = join else {
            owner_task.abort();
            panic!("expected forwarded join after owner arm");
        };

        // Owner must be in the echo loop before we send payload bytes.
        tokio::time::timeout(Duration::from_secs(5), echo_ready_rx)
            .await
            .expect("owner echo ready")
            .expect("owner echo task dropped");

        let community = CommunityId::from_uuid(community_id);
        if let Err(e) = stream.send_bytes(community, payload.as_bytes()).await {
            owner_task.abort();
            return (
                StatusCode::BAD_GATEWAY,
                serde_json::json!({"error": format!("send failed: {e}")}),
            );
        }
        let echoed = tokio::time::timeout(ECHO_TIMEOUT, stream.recv_validated(&directory)).await;
        owner_task.abort();
        match echoed {
            Ok(Ok(Some(ReliableFrame::Data(bytes)))) => (
                StatusCode::OK,
                serde_json::json!({
                    "outcome": "forwarded",
                    "generation": lease.generation,
                    "owner_runtime_id": lease.owner_runtime_id.to_string(),
                    "echoed_payload": String::from_utf8_lossy(&bytes),
                }),
            ),
            Ok(Ok(Some(ReliableFrame::Goodbye(reason)))) => (
                StatusCode::BAD_GATEWAY,
                serde_json::json!({"error": format!("owner sent goodbye: {reason:?}")}),
            ),
            Ok(Ok(None)) => (
                StatusCode::BAD_GATEWAY,
                serde_json::json!({"error": "stream closed before echo"}),
            ),
            Ok(Err(e)) => (
                StatusCode::BAD_GATEWAY,
                serde_json::json!({"error": format!("recv failed: {e}")}),
            ),
            Err(_) => (
                StatusCode::GATEWAY_TIMEOUT,
                serde_json::json!({"error": "timed out waiting for echo"}),
            ),
        }
    }

    /// First post for a session acquires the fenced lease and reports `owned`.
    #[tokio::test]
    async fn demo_join_owned_arm_reports_generation() {
        let Some(directory) = redis_directory_for_mesh_demo().await else {
            return;
        };
        let community_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        directory
            .clear_session_keys(CommunityId::from_uuid(community_id), session_id)
            .await
            .expect("clear session keys on isolated redis");
        let router = ReliableStreamRouter::new(
            directory.clone(),
            std::sync::Arc::new(NoopTransport),
            RuntimeId([7; 32]),
        );
        let resp = run_demo_join(
            &router,
            &directory,
            DemoEchoRequest {
                community_id,
                session_id,
                payload: "unused".into(),
            },
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["outcome"], "owned");
        assert!(body["generation"].as_u64().is_some());
    }

    /// Second runtime forwards to the owner and round-trips the payload
    /// through the owner-side echo consumer (`recv_validated` + `send_bytes`),
    /// end to end over a real mesh stream pair.
    #[tokio::test]
    async fn demo_join_forwarded_arm_round_trips_echo() {
        let Some(directory) = redis_directory_for_mesh_demo().await else {
            return;
        };
        let (status, body) = forwarded_echo_round_trip(
            directory,
            Uuid::new_v4(),
            Uuid::new_v4(),
            "mesh echo evidence",
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "forwarded echo must succeed on isolated redis (db={MESH_DEMO_TEST_REDIS_DB}); body={body}"
        );
        assert_eq!(body["outcome"], "forwarded");
        assert_eq!(body["echoed_payload"], "mesh echo evidence");
    }

    /// Targeted proof: leftover / concurrent lease noise on shared Redis DB 0
    /// must not break the forwarded arm when the demo fixture uses DB 15.
    #[tokio::test]
    async fn demo_forwarded_arm_isolated_from_shared_db0_lease_noise() {
        let Some(directory) = redis_directory_for_mesh_demo().await else {
            return;
        };
        let Some(db0_pool) = shared_db0_pool().await else {
            eprintln!("mesh_demo isolation proof skip: shared db0 redis unavailable");
            return;
        };

        let community_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let community = CommunityId::from_uuid(community_id);

        // Poison shared DB 0 with a conflicting owner for the same ids.
        let poison = SessionDirectory::with_lease_ttl(db0_pool, Duration::from_secs(120));
        poison
            .acquire(
                community,
                session_id,
                RuntimeId([0xDB; 32]),
                Profile::ReliableStream,
            )
            .await
            .expect("plant db0 poison lease");

        let (status, body) =
            forwarded_echo_round_trip(directory, community_id, session_id, "db0-noise-immune")
                .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "db0 poison lease must not affect isolated mesh-demo redis; body={body}"
        );
        assert_eq!(body["outcome"], "forwarded");
        assert_eq!(body["echoed_payload"], "db0-noise-immune");

        // Best-effort cleanup of the intentional DB0 poison (do not FLUSHDB).
        let _ = poison.clear_session_keys(community, session_id).await;
    }

    /// Shared Redis sensitivity: wiping the fenced lease under an active
    /// forwarded stream must fail closed (no echoed Data). Documents why the
    /// demo fixture cannot share DB 0 with suites that DEL/expire mesh keys.
    #[tokio::test]
    async fn demo_forwarded_arm_fails_closed_when_lease_disappears() {
        let Some(directory) = redis_directory_for_mesh_demo().await else {
            return;
        };
        let community_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let community = CommunityId::from_uuid(community_id);
        directory
            .clear_session_keys(community, session_id)
            .await
            .expect("clear session keys");

        let bind = || "127.0.0.1:0".parse().unwrap();
        let local_endpoint = MeshEndpoint::bind(bind()).await.unwrap();
        let owner_endpoint = MeshEndpoint::bind(bind()).await.unwrap();
        let owner_runtime = owner_endpoint.runtime_id();
        let owner_addr = owner_endpoint.addr();

        let accept_endpoint = owner_endpoint.clone();
        let accept = tokio::spawn(async move { accept_endpoint.accept().await.unwrap().unwrap() });
        let local_peer = local_endpoint.connect(owner_addr).await.unwrap();
        let owner_peer = accept.await.unwrap();

        let owner_router = ReliableStreamRouter::new(
            directory.clone(),
            std::sync::Arc::new(NoopTransport),
            owner_runtime,
        );
        assert_eq!(
            run_demo_join(
                &owner_router,
                &directory,
                DemoEchoRequest {
                    community_id,
                    session_id,
                    payload: "unused".into(),
                },
            )
            .await
            .status(),
            StatusCode::OK
        );

        let echo_directory = directory.clone();
        let owner_task = tokio::spawn(async move {
            let mut stream = owner_peer.accept_bi().await.unwrap();
            let hello = match stream.recv_frame().await.unwrap().unwrap() {
                MeshStreamFrame::Hello(h) => h,
                other => panic!("expected hello, got {other:?}"),
            };
            let router = ReliableStreamRouter::new(
                echo_directory.clone(),
                std::sync::Arc::new(NoopTransport),
                owner_runtime,
            );
            let inbound = router
                .accept_inbound(hello.sender, hello, stream)
                .await
                .expect("accept_inbound");
            crate::mesh_boot::run_demo_echo(
                echo_directory,
                inbound,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .await;
        });

        let local_router = ReliableStreamRouter::new(
            directory.clone(),
            std::sync::Arc::new(DirectTransport { peer: local_peer }),
            local_endpoint.runtime_id(),
        );
        let join_fwd = local_router
            .join(community, session_id)
            .await
            .expect("forward join");
        let crate::tunnel::reliable::ReliableJoin::Forwarded { mut stream, .. } = join_fwd else {
            owner_task.abort();
            panic!("expected forwarded join");
        };

        directory
            .clear_session_keys(community, session_id)
            .await
            .expect("wipe lease under active forward");
        let _ = stream.send_bytes(community, b"should-not-echo").await;
        let echoed =
            tokio::time::timeout(Duration::from_secs(2), stream.recv_validated(&directory)).await;
        let got_data = matches!(
            echoed,
            Ok(Ok(Some(crate::tunnel::reliable::ReliableFrame::Data(_))))
        );
        assert!(
            !got_data,
            "lease wipe must prevent a successful echoed Data frame"
        );
        owner_task.abort();
    }
}
