//! WebSocket connection lifecycle: semaphore → challenge → recv/send/heartbeat loops → cleanup.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use buzz_auth::{generate_challenge, AuthContext};
use buzz_core::tenant::TenantContext;
use nostr::Filter;

use crate::handlers;
use crate::protocol::{ClientMessage, RelayMessage};
use crate::rejection::{enforce_ws_admission, request_rejection_message, RejectionTarget};
use crate::state::{
    run_registered_community_connection, AppState, CommunityConnectionControl,
    CommunityDisconnectReason,
};

/// Maximum time a new socket may hold a connection slot without completing NIP-42 auth.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared mutable subscription map for a single WebSocket connection.
pub(crate) type ConnectionSubscriptions = Arc<Mutex<HashMap<String, Vec<Filter>>>>;

/// Request for the writer to flush a restart close and report the result.
pub(crate) struct RestartClose {
    pub(crate) flushed: tokio::sync::oneshot::Sender<bool>,
}

/// Maximum outbound data frames buffered into the websocket sink before one flush.
const MAX_WS_SEND_BATCH: usize = 64;

/// NIP-42 authentication state for a single connection.
#[derive(Debug, Clone)]
pub enum AuthState {
    /// Challenge has been sent; awaiting a signed AUTH event from the client.
    Pending {
        /// The random challenge string sent to the client.
        challenge: String,
    },
    /// Client has successfully authenticated.
    Authenticated(AuthContext),
    /// Authentication attempt was rejected.
    Failed,
}

/// Per-connection state split by access pattern:
/// - `auth_state`: RwLock (read-heavy after initial auth)
/// - `subscriptions`: Mutex (write-heavy during REQ/CLOSE)
/// - `send_tx`, `ctrl_tx`, `cancel`: outside any lock (Clone+Send, no coordination needed)
pub struct ConnectionState {
    /// Unique identifier for this connection.
    pub conn_id: Uuid,
    /// The community this connection is bound to, resolved from the connection
    /// host at row zero (before any frame is read) and never overridable by
    /// client-supplied input. Every handler reads tenant scope from here.
    pub tenant: TenantContext,
    /// Remote socket address of the client.
    pub remote_addr: SocketAddr,
    /// Current NIP-42 authentication state.
    pub auth_state: RwLock<AuthState>,
    /// Active subscriptions keyed by subscription ID.
    pub subscriptions: ConnectionSubscriptions,
    /// Sender for outbound data messages (EVENT, NOTICE, OK, etc.).
    pub send_tx: mpsc::Sender<WsMessage>,
    /// Sender for outbound control frames (Pong, Close).
    /// Separate channel with priority drain — if this channel fills too,
    /// the connection is closed (writer is completely stalled).
    pub ctrl_tx: mpsc::Sender<WsMessage>,
    /// Token used to signal graceful shutdown of this connection's tasks.
    pub cancel: CancellationToken,
    /// Consecutive buffer-full events. Cancel only after `grace_limit`.
    /// Shared with `ConnectionManager::ConnEntry` so both direct sends and
    /// fan-out broadcasts track the same counter.
    pub backpressure_count: Arc<AtomicU8>,
    /// Configurable slow-client grace limit (from `Config::slow_client_grace_limit`).
    pub grace_limit: u8,
}

impl ConnectionState {
    /// Sends a data message to this connection's outbound channel.
    ///
    /// On a full buffer, increments the backpressure counter. The first
    /// `grace_limit` occurrences log a warning; sustained backpressure
    /// cancels the connection to prevent unbounded memory growth.
    pub fn send(&self, msg: String) -> bool {
        match self.send_tx.try_send(WsMessage::Text(msg.into())) {
            Ok(_) => {
                // Successful send resets the grace counter.
                self.backpressure_count.store(0, Ordering::Relaxed);
                true
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                let count = self.backpressure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.grace_limit {
                    warn!(conn_id = %self.conn_id, count, "sustained backpressure — closing slow client");
                    metrics::counter!("buzz_ws_backpressure_disconnects_total").increment(1);
                    self.cancel.cancel();
                } else {
                    warn!(conn_id = %self.conn_id, count, grace = self.grace_limit, "send buffer full — grace {count}/{}", self.grace_limit);
                }
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                debug!(conn_id = %self.conn_id, "send channel closed");
                false
            }
        }
    }
}

/// Entry point for a new WebSocket connection.
///
/// Acquires a connection semaphore permit, sends the NIP-42 AUTH challenge,
/// then drives the send, heartbeat, and receive loops until the connection closes.
pub async fn handle_connection(
    socket: WebSocket,
    state: Arc<AppState>,
    addr: SocketAddr,
    tenant: TenantContext,
) {
    let conn_id = Uuid::new_v4();
    let cancel = CancellationToken::new();
    let control = CommunityConnectionControl::new(cancel);
    let community_id = tenant.community();
    let registry = Arc::clone(&state.community_connections);
    let check_state = Arc::clone(&state);
    let run_state = Arc::clone(&state);
    run_registered_community_connection(
        &registry,
        conn_id,
        community_id,
        control,
        move || async move { check_state.db.is_community_active(community_id).await },
        move |control| handle_active_connection(socket, run_state, addr, tenant, conn_id, control),
    )
    .await;
}

async fn handle_active_connection(
    socket: WebSocket,
    state: Arc<AppState>,
    addr: SocketAddr,
    tenant: TenantContext,
    conn_id: Uuid,
    control: CommunityConnectionControl,
) {
    let cancel = control.cancellation_token();
    let disconnect_reason = control.disconnect_reason();
    let permit = match state.conn_semaphore.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            warn!("Connection limit reached, rejecting {addr}");
            return;
        }
    };

    let challenge = generate_challenge();

    let (tx, rx) = mpsc::channel::<WsMessage>(state.config.send_buffer_size);
    // Control channel for Pong/Close — small capacity, guaranteed delivery
    // even when the data buffer is full.
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<WsMessage>(8);

    // Dedicated restart-close channel carries a flush acknowledgement. Keeping
    // ordinary control frames unchanged avoids coupling heartbeat/ban traffic
    // to graceful-shutdown delivery tracking.
    let (restart_tx, restart_rx) = mpsc::channel::<RestartClose>(1);

    let backpressure_count = Arc::new(AtomicU8::new(0));
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));

    let conn = Arc::new(ConnectionState {
        conn_id,
        tenant,
        remote_addr: addr,
        auth_state: RwLock::new(AuthState::Pending {
            challenge: challenge.clone(),
        }),
        subscriptions: Arc::clone(&subscriptions),
        send_tx: tx.clone(),
        ctrl_tx: ctrl_tx.clone(),
        cancel: cancel.clone(),
        backpressure_count: Arc::clone(&backpressure_count),
        grace_limit: state.config.slow_client_grace_limit,
    });

    info!(conn_id = %conn_id, addr = %addr, "WebSocket connection established");
    metrics::counter!(
        "buzz_ws_connections_total",
        "community" => conn.tenant.host().to_owned()
    )
    .increment(1);

    let challenge_msg = RelayMessage::auth_challenge(&challenge);
    if tx
        .send(WsMessage::Text(challenge_msg.into()))
        .await
        .is_err()
    {
        warn!(conn_id = %conn_id, "Failed to send AUTH challenge — client disconnected immediately");
        return;
    }

    // Gauge incremented AFTER challenge send succeeds — early disconnects
    // don't leak. Decremented in the cleanup path below.
    metrics::gauge!("buzz_ws_connections_active").increment(1.0);

    // Register after challenge succeeds — avoids leaked entries on early disconnect.
    state.conn_manager.register(
        conn_id,
        tx.clone(),
        ctrl_tx.clone(),
        Some(restart_tx),
        cancel.clone(),
        conn.tenant.community(),
        Arc::clone(&backpressure_count),
        subscriptions,
        state.config.slow_client_grace_limit,
    );

    let (ws_send, ws_recv) = socket.split();

    let send_cancel = cancel.child_token();
    let send_task = tokio::spawn(send_loop(
        ws_send,
        rx,
        ctrl_rx,
        restart_rx,
        send_cancel,
        disconnect_reason,
    ));

    let missed_pongs = Arc::new(AtomicU8::new(0));
    let heartbeat_cancel = cancel.clone();
    let heartbeat_task = tokio::spawn(heartbeat_loop(
        ctrl_tx,
        Arc::clone(&missed_pongs),
        heartbeat_cancel,
    ));

    let auth_timeout_conn = Arc::clone(&conn);
    let auth_timeout_cancel = cancel.clone();
    let auth_timeout_task = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(AUTH_TIMEOUT) => {
                let authenticated = matches!(
                    *auth_timeout_conn.auth_state.read().await,
                    AuthState::Authenticated(_)
                );
                if !authenticated {
                    warn!(
                        conn_id = %auth_timeout_conn.conn_id,
                        timeout_secs = AUTH_TIMEOUT.as_secs(),
                        "NIP-42 auth timeout — closing connection"
                    );
                    metrics::counter!("buzz_ws_auth_timeouts_total").increment(1);
                    auth_timeout_cancel.cancel();
                }
            }
            _ = auth_timeout_cancel.cancelled() => {}
        }
    });

    recv_loop(
        ws_recv,
        Arc::clone(&conn),
        Arc::clone(&state),
        Arc::clone(&missed_pongs),
        cancel.clone(),
    )
    .await;

    cancel.cancel();
    let _ = send_task.await;
    let _ = heartbeat_task.await;
    let _ = auth_timeout_task.await;

    for removed in state.sub_registry.remove_connection(conn.conn_id) {
        if removed.scope.is_global() {
            state
                .pubsub
                .release_topic(&conn.tenant, buzz_pubsub::EventTopic::Global)
                .await;
        }
        for &channel_id in removed.scope.channel_ids() {
            state
                .pubsub
                .release_topic(&conn.tenant, buzz_pubsub::EventTopic::Channel(channel_id))
                .await;
        }
    }
    state.conn_manager.deregister(conn.conn_id);
    if let AuthState::Authenticated(ref auth_ctx) = *conn.auth_state.read().await {
        let remaining = state.conn_manager.connection_ids_for_pubkey_in_community(
            conn.tenant.community(),
            auth_ctx.pubkey.to_bytes().as_slice(),
        );
        if remaining.is_empty() {
            let _ = state
                .pubsub
                .clear_presence(&conn.tenant, &auth_ctx.pubkey)
                .await;
        }
    }
    metrics::gauge!("buzz_ws_connections_active").decrement(1.0);
    info!(conn_id = %conn_id, addr = %addr, "WebSocket connection closed");

    drop(permit);
}

/// Outbound send loop with control-frame priority.
///
/// Control frames (Pong, Close) are drained first on every iteration,
/// giving them priority over data frames. If the underlying socket writer
/// is stalled, control frames queue in the small ctrl_rx buffer; callers
/// treat a full control channel as terminal (Bug 7 fix).
async fn send_loop(
    ws_send: futures_util::stream::SplitSink<WebSocket, WsMessage>,
    data_rx: mpsc::Receiver<WsMessage>,
    ctrl_rx: mpsc::Receiver<WsMessage>,
    restart_rx: mpsc::Receiver<RestartClose>,
    cancel: CancellationToken,
    disconnect_reason: watch::Receiver<Option<CommunityDisconnectReason>>,
) {
    send_loop_inner(
        ws_send,
        data_rx,
        ctrl_rx,
        restart_rx,
        cancel,
        disconnect_reason,
    )
    .await;
}

async fn send_loop_inner<S>(
    mut ws_send: S,
    mut data_rx: mpsc::Receiver<WsMessage>,
    mut ctrl_rx: mpsc::Receiver<WsMessage>,
    mut restart_rx: mpsc::Receiver<RestartClose>,
    cancel: CancellationToken,
    disconnect_reason: watch::Receiver<Option<CommunityDisconnectReason>>,
) where
    S: Sink<WsMessage> + Unpin,
{
    loop {
        // Priority: drain all pending control frames before data.
        while let Ok(ctrl_msg) = ctrl_rx.try_recv() {
            if ws_send.send(ctrl_msg).await.is_err() {
                return;
            }
        }

        tokio::select! {
            // Biased: restart > cancel > ordinary control > data. A restart
            // command owns shutdown delivery and must flush its 1012 before
            // cancellation can fall back to an unacknowledged close.
            biased;
            Some(restart) = restart_rx.recv() => {
                let sent = ws_send
                    .send(WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                        code: axum::extract::ws::close_code::RESTART,
                        reason: axum::extract::ws::Utf8Bytes::from_static("relay restarting"),
                    })))
                    .await
                    .is_ok();
                let _ = restart.flushed.send(sent);
                break;
            }
            _ = cancel.cancelled() => {
                // Drain any queued control frames before closing. A ban
                // disconnect queues its `OK false "blocked: …"` reason frame on
                // ctrl and then cancels; without this drain the biased branch
                // would send Close first and the client would never learn why
                // (the top-of-loop drain does not run again after we break).
                // This makes "queue frame on ctrl, then cancel" a safe idiom.
                while let Ok(ctrl_msg) = ctrl_rx.try_recv() {
                    if ws_send.send(ctrl_msg).await.is_err() {
                        break;
                    }
                }
                let close = disconnect_reason
                    .borrow()
                    .map_or(WsMessage::Close(None), |reason| reason.close_message());
                let _ = ws_send.send(close).await;
                break;
            }
            Some(ctrl_msg) = ctrl_rx.recv() => {
                if ws_send.send(ctrl_msg).await.is_err() {
                    break;
                }
            }
            Some(msg) = data_rx.recv() => {
                let mut batched = 1usize;
                if ws_send.feed(msg).await.is_err() {
                    break;
                }

                while batched < MAX_WS_SEND_BATCH {
                    match data_rx.try_recv() {
                        Ok(next) => {
                            if ws_send.feed(next).await.is_err() {
                                return;
                            }
                            batched += 1;
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                if ws_send.flush().await.is_err() {
                    break;
                }
                metrics::histogram!("buzz_ws_send_batch_size").record(batched as f64);
            }
        }
    }
}

/// 3 missed pongs → disconnect.
///
/// Sends Ping through the control channel so it isn't blocked by a full
/// data buffer. Uses `try_send` to keep the select loop responsive to
/// cancellation — a full control channel means the writer is stalled.
async fn heartbeat_loop(
    ctrl_tx: mpsc::Sender<WsMessage>,
    missed_pongs: Arc<AtomicU8>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // fetch_add returns the *previous* value before incrementing:
                //   prev=0 → now 1 (first miss)
                //   prev=1 → now 2 (second miss)
                //   prev=2 → now 3 (third miss → disconnect)
                let missed = missed_pongs.fetch_add(1, Ordering::Relaxed);
                if missed >= 2 {
                    warn!("3 missed pongs — closing connection");
                    cancel.cancel();
                    break;
                }
                if ctrl_tx.try_send(WsMessage::Ping(axum::body::Bytes::new())).is_err() {
                    warn!("control channel full — cannot send Ping, closing");
                    cancel.cancel();
                    break;
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

async fn recv_loop(
    mut ws_recv: futures_util::stream::SplitStream<WebSocket>,
    conn: Arc<ConnectionState>,
    state: Arc<AppState>,
    missed_pongs: Arc<AtomicU8>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            msg = ws_recv.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        let max_frame_bytes = state.config.max_frame_bytes;
                        if text.len() > max_frame_bytes {
                            warn!(
                                conn_id = %conn.conn_id,
                                bytes = text.len(),
                                max_frame_bytes,
                                "frame too large — disconnecting"
                            );
                            conn.send(format!(
                                r#"["NOTICE","error: frame too large ({} bytes, limit {})"]"#,
                                text.len(),
                                max_frame_bytes
                            ));
                            break;
                        }
                        trace!(len = text.len(), "frame received");
                        handle_text_message(text.to_string(), Arc::clone(&conn), Arc::clone(&state)).await;
                    }
                    Some(Ok(WsMessage::Binary(bytes))) => {
                        let max_frame_bytes = state.config.max_frame_bytes;
                        if bytes.len() > max_frame_bytes {
                            warn!(
                                conn_id = %conn.conn_id,
                                bytes = bytes.len(),
                                max_frame_bytes,
                                "binary frame too large — disconnecting"
                            );
                            conn.send(format!(
                                r#"["NOTICE","error: binary frame too large ({} bytes, limit {})"]"#,
                                bytes.len(),
                                max_frame_bytes
                            ));
                            break;
                        }
                        // Binary frames: attempt UTF-8 decode and treat as text. Some clients
                        // (notably certain Nostr libraries) send text payloads in binary frames.
                        // NIP-01 is text-only, but accepting binary is a common relay extension.
                        if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                            handle_text_message(text, Arc::clone(&conn), Arc::clone(&state)).await;
                        }
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        missed_pongs.store(0, Ordering::Relaxed);
                    }
                    Some(Ok(WsMessage::Ping(data))) => {
                        // Send Pong through the control channel — priority
                        // delivery even when the data buffer is full (Bug 7 fix).
                        if conn.ctrl_tx.try_send(WsMessage::Pong(data)).is_err() {
                            // Control channel full means the socket writer is
                            // completely stalled — treat as terminal.
                            warn!(conn_id = %conn.conn_id, "control channel full — cannot send Pong, closing");
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | None => {
                        debug!("WebSocket closed by client");
                        break;
                    }
                    Some(Err(e)) => {
                        debug!("WebSocket error: {e}");
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

async fn handle_text_message(text: String, conn: Arc<ConnectionState>, state: Arc<AppState>) {
    let msg = match ClientMessage::parse(&text) {
        Ok(m) => m,
        Err(e) => {
            conn.send(RelayMessage::notice(&format!("invalid message: {e}")));
            return;
        }
    };

    if !enforce_ws_admission(&msg, &conn, &state).await {
        return;
    }

    match msg {
        ClientMessage::Auth(event) => {
            // Auth is synchronous in the WS loop — no span context is lost.
            let span = tracing::info_span!("ws.auth", conn_id = %conn.conn_id);
            handlers::auth::handle_auth(event, Arc::clone(&conn), Arc::clone(&state))
                .instrument(span)
                .await;
        }
        ClientMessage::Event(event) => {
            let conn = Arc::clone(&conn);
            let state = Arc::clone(&state);
            let permit = match state.handler_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // Correlate to the event id: a bare NOTICE here strands the
                    // client's pending publish exactly as an over-quota one did.
                    conn.send(request_rejection_message(
                        RejectionTarget::Event(event.id),
                        "rate-limited: too many concurrent requests",
                    ));
                    return;
                }
            };
            // Capture the parent span BEFORE the spawn so it is propagated into
            // the spawned future.  A bare `tokio::spawn` drops tracing context.
            let span = tracing::info_span!(
                "ws.event",
                conn_id = %conn.conn_id,
                event_id = tracing::field::Empty,
                kind = tracing::field::Empty,
            );
            tokio::spawn(
                async move {
                    handlers::event::handle_event(event, conn, state).await;
                    drop(permit);
                }
                .instrument(span),
            );
        }
        ClientMessage::Req {
            sub_id,
            filters,
            before_ids,
        } => {
            let conn = Arc::clone(&conn);
            let state = Arc::clone(&state);
            let permit = match state.handler_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    conn.send(request_rejection_message(
                        RejectionTarget::Subscription(&sub_id),
                        "rate-limited: too many concurrent requests",
                    ));
                    return;
                }
            };
            let span = tracing::info_span!("ws.req", conn_id = %conn.conn_id, sub_id = %sub_id);
            tokio::spawn(
                async move {
                    handlers::req::handle_req(sub_id, filters, before_ids, conn, state).await;
                    drop(permit);
                }
                .instrument(span),
            );
        }
        ClientMessage::Count { sub_id, filters } => {
            let conn = Arc::clone(&conn);
            let state = Arc::clone(&state);
            let permit = match state.handler_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    conn.send(request_rejection_message(
                        RejectionTarget::Subscription(&sub_id),
                        "rate-limited: too many concurrent requests",
                    ));
                    return;
                }
            };
            let span = tracing::info_span!("ws.count", conn_id = %conn.conn_id, sub_id = %sub_id);
            tokio::spawn(
                async move {
                    handlers::count::handle_count(sub_id, filters, conn, state).await;
                    drop(permit);
                }
                .instrument(span),
            );
        }
        ClientMessage::Close(sub_id) => {
            handlers::close::handle_close(sub_id, Arc::clone(&conn), Arc::clone(&state)).await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use buzz_auth::AuthMethod;
    use nostr::{EventBuilder, Keys, Kind};

    /// A connection whose outbound frames a test can read back.
    ///
    /// Lives here, next to `ConnectionState`, so the crate has one place that
    /// knows how to build one. Shared with `crate::rejection`'s tests.
    pub(crate) fn test_conn_with_auth(
        auth: AuthState,
    ) -> (Arc<ConnectionState>, mpsc::Receiver<WsMessage>) {
        let (send_tx, send_rx) = mpsc::channel(4);
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(4);
        let conn = ConnectionState {
            conn_id: Uuid::new_v4(),
            tenant: TenantContext::resolved(
                buzz_core::tenant::CommunityId::from_uuid(Uuid::nil()),
                "test.local".to_string(),
            ),
            remote_addr: "127.0.0.1:1234".parse().expect("socket addr"),
            auth_state: RwLock::new(auth),
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            send_tx,
            ctrl_tx,
            cancel: CancellationToken::new(),
            backpressure_count: Arc::new(AtomicU8::new(0)),
            grace_limit: 3,
        };
        (Arc::new(conn), send_rx)
    }

    /// An authenticated connection — the only state admission quotas apply to.
    pub(crate) fn authenticated_state() -> AuthState {
        AuthState::Authenticated(AuthContext {
            pubkey: Keys::generate().public_key(),
            scopes: Vec::new(),
            channel_ids: None,
            auth_method: AuthMethod::Nip42,
            agent_owner_pubkey: None,
        })
    }

    pub(crate) fn read_frame(rx: &mut mpsc::Receiver<WsMessage>) -> serde_json::Value {
        match rx.try_recv().expect("a frame was sent") {
            WsMessage::Text(text) => serde_json::from_str(&text).expect("valid JSON frame"),
            other => panic!("unexpected websocket message: {other:?}"),
        }
    }

    /// Drives the real `handle_text_message` with every handler permit held, so
    /// the EVENT saturation branch is reached through production dispatch rather
    /// than by calling its helpers directly.
    ///
    /// This must go through `handle_text_message`: a test that renders the
    /// rejection frame itself stays green when the call site inside the match
    /// arm is reverted to a bare `NOTICE`.
    #[tokio::test]
    async fn saturated_handler_rejects_an_event_on_the_ok_channel() {
        let state = crate::state::tests::test_state().await;
        // An unauthenticated connection skips the admission quotas, so the
        // semaphore is the only gate the frame can trip.
        let (conn, mut rx) = test_conn_with_auth(AuthState::Failed);

        let permits = state.handler_semaphore.available_permits();
        let _held = Arc::clone(&state.handler_semaphore)
            .acquire_many_owned(permits as u32)
            .await
            .expect("hold every handler permit");

        let event = EventBuilder::new(Kind::TextNote, "hello")
            .sign_with_keys(&Keys::generate())
            .expect("sign event");
        let event_id = event.id.to_hex();
        let raw = serde_json::json!(["EVENT", event]).to_string();

        handle_text_message(raw, Arc::clone(&conn), Arc::clone(&state)).await;

        let frame = read_frame(&mut rx);
        assert_eq!(
            frame[0], "OK",
            "an EVENT turned away for handler saturation must be rejected on the \
             OK channel — a NOTICE carries no event id, so the client's pending \
             publish cannot be settled and the send only times out"
        );
        assert_eq!(frame[1], event_id);
        assert_eq!(frame[2], false);
        assert_eq!(frame[3], "rate-limited: too many concurrent requests");
    }

    // ---------------------------------------------------------------------
    // root-jk1sw Major 1 — isolated multi-agent mention-burst analogue.
    //
    // The prior candidate proved only admission *classification* while every
    // handler permit was held: under that condition subscriptions necessarily
    // close, so it could not speak to the bead's acceptance criterion
    // ("subscriptions stay up through a multi-agent mention burst without
    // database-error closes").
    //
    // This fixture inverts that: capacity is configured the way the immutable
    // unit ships it (pool 50 / handlers 45), several agent connections each open
    // several subscriptions, and a burst of mention EVENTs is dispatched
    // concurrently through the real `handle_text_message`. The assertions are on
    // survival and on the absence of `error: database error`, with explicit
    // nonzero denominators printed so a reviewer can see what was actually
    // exercised.
    //
    // Live relay proof remains Gate 4; this is the in-repo analogue.
    // ---------------------------------------------------------------------

    /// Agents in the burst.
    const BURST_AGENTS: usize = 8;
    /// Subscriptions each agent opens.
    const BURST_SUBS_PER_AGENT: usize = 3;
    /// Mention events published per agent.
    const BURST_EVENTS_PER_AGENT: usize = 8;

    /// An authenticated connection bound to a specific agent keypair and to the
    /// community the burst fixture actually seeded, so REQ/EVENT resolve against
    /// real tenant rows instead of the nil community.
    fn burst_conn(
        keys: &Keys,
        community: buzz_core::tenant::CommunityId,
    ) -> (Arc<ConnectionState>, mpsc::Receiver<WsMessage>) {
        let (send_tx, send_rx) = mpsc::channel(1024);
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(64);
        let conn = ConnectionState {
            conn_id: Uuid::new_v4(),
            tenant: TenantContext::resolved(community, "relay.example".to_string()),
            remote_addr: "127.0.0.1:1234".parse().expect("socket addr"),
            auth_state: RwLock::new(AuthState::Authenticated(AuthContext {
                pubkey: keys.public_key(),
                scopes: Vec::new(),
                channel_ids: None,
                auth_method: AuthMethod::Nip42,
                agent_owner_pubkey: None,
            })),
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            send_tx,
            ctrl_tx,
            cancel: CancellationToken::new(),
            backpressure_count: Arc::new(AtomicU8::new(0)),
            grace_limit: 3,
        };
        (Arc::new(conn), send_rx)
    }

    /// Isolated logical Redis DB for burst tests (root-kd5gc convention:
    /// never share DB 0 with the live relay or other suites).
    const BURST_REDIS_DB: u32 = 14;

    fn burst_redis_url() -> String {
        if let Ok(url) = std::env::var("BUZZ_TEST_BURST_REDIS_URL") {
            let trimmed = url.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        format!("redis://127.0.0.1:6379/{BURST_REDIS_DB}")
    }

    fn burst_database_url() -> String {
        // Same resolution order as the root-6mu08 media fixture.
        const TEST_DB_URL: &str = "postgres://buzz:buzz_dev@localhost:5432/buzz"; // sadscan:disable np.postgres.1 -- local test-only credentials
        std::env::var("BUZZ_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("TEST_DATABASE_URL"))
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| TEST_DB_URL.to_string())
    }

    /// Relay state with *working* backing stores and production-shaped capacity.
    ///
    /// Returns `None` with an explicit reason when the isolated dependencies are
    /// unavailable, so the burst never silently degrades into a vacuous pass.
    async fn burst_state() -> Option<(Arc<crate::state::AppState>, buzz_core::tenant::CommunityId)>
    {
        let mut config = crate::config::Config::from_env().expect("default config loads");
        config.require_relay_membership = false;
        config.database_url = burst_database_url();
        config.redis_url = burst_redis_url();
        // Exactly what deploy/host/pepper/buzz-relay.service ships.
        config.db_pool_size = 50;
        config.max_concurrent_handlers = crate::config::handler_capacity_for_pool(50);
        assert_eq!(config.max_concurrent_handlers, 45);

        // Build the pool at the configured size, so the fixture actually
        // exercises the capacity relationship it claims to. Connecting with
        // sqlx's defaults here would silently ignore `db_pool_size` and make the
        // burst insensitive to the very invariant under test.
        let pool = match sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.db_pool_size)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&config.database_url)
            .await
        {
            Ok(pool) => pool,
            Err(err) => {
                eprintln!("burst skip: test postgres unavailable: {err}");
                return None;
            }
        };
        let db = buzz_db::Db::from_pool(pool.clone());
        if let Err(err) = db.migrate().await {
            eprintln!("burst skip: migrate failed: {err}");
            return None;
        }
        let community = match db.ensure_configured_community("relay.example").await {
            Ok(record) => record.id,
            Err(err) => {
                eprintln!("burst skip: seed community failed: {err}");
                return None;
            }
        };
        let redis_pool = match deadpool_redis::Config::from_url(&config.redis_url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        {
            Ok(p) => p,
            Err(err) => {
                eprintln!("burst skip: redis pool create failed (db={BURST_REDIS_DB}): {err}");
                return None;
            }
        };
        match redis_pool.get().await {
            Ok(mut conn) => {
                if redis::cmd("PING")
                    .query_async::<String>(&mut *conn)
                    .await
                    .is_err()
                {
                    eprintln!("burst skip: redis PING failed (db={BURST_REDIS_DB})");
                    return None;
                }
            }
            Err(err) => {
                eprintln!("burst skip: redis unavailable (db={BURST_REDIS_DB}): {err}");
                return None;
            }
        }
        let pubsub =
            match buzz_pubsub::PubSubManager::new(&config.redis_url, redis_pool.clone()).await {
                Ok(p) => Arc::new(p),
                Err(err) => {
                    eprintln!("burst skip: pubsub manager failed: {err}");
                    return None;
                }
            };
        let audit = buzz_audit::AuditService::new(pool.clone());
        let auth = buzz_auth::AuthService::new(config.auth.clone());
        let search = buzz_search::SearchService::new(pool.clone());
        let workflow_engine = Arc::new(buzz_workflow::WorkflowEngine::new(
            db.clone(),
            buzz_workflow::WorkflowConfig::default(),
        ));
        let media_storage = buzz_media::MediaStorage::new(&config.media).expect("media storage");
        let (state, _audit_shutdown) = crate::state::AppState::new(
            config,
            db,
            redis_pool,
            audit,
            pubsub,
            auth,
            search,
            workflow_engine,
            nostr::Keys::generate(),
            media_storage,
        );
        Some((Arc::new(state), community))
    }

    /// Drain every frame a connection has been sent so far.
    fn drain_frames(rx: &mut mpsc::Receiver<WsMessage>) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let WsMessage::Text(text) = msg {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                    out.push(value);
                }
            }
        }
        out
    }

    /// AC proof: subscriptions survive a concurrent multi-agent mention burst
    /// and nothing closes with `error: database error`.
    #[tokio::test]
    async fn multi_agent_mention_burst_keeps_subscriptions_up_without_database_errors() {
        let Some((state, community)) = burst_state().await else {
            // A skipped burst must never read as a pass in CI. Setting
            // BUZZ_TEST_BURST_REQUIRE=1 turns an unavailable dependency into a
            // failure instead of a silent green.
            assert!(
                std::env::var("BUZZ_TEST_BURST_REQUIRE").as_deref() != Ok("1"),
                "BUZZ_TEST_BURST_REQUIRE=1 but the burst dependencies were unavailable"
            );
            return;
        };

        // Every agent is a distinct connection with its own keypair, mirroring
        // the buzz-acp fleet shape that produced the incident.
        let mut agents = Vec::new();
        for _ in 0..BURST_AGENTS {
            let keys = Keys::generate();
            let (conn, rx) = burst_conn(&keys, community);
            agents.push((keys, conn, rx));
        }

        // Phase 1 — open subscriptions.
        let mut opened = 0usize;
        for (agent_idx, (_keys, conn, _rx)) in agents.iter().enumerate() {
            for sub_idx in 0..BURST_SUBS_PER_AGENT {
                let sub_id = format!("burst-a{agent_idx}-s{sub_idx}");
                let raw =
                    serde_json::json!(["REQ", sub_id, {"kinds": [1], "limit": 1}]).to_string();
                handle_text_message(raw, Arc::clone(conn), Arc::clone(&state)).await;
                opened += 1;
            }
        }
        assert!(opened > 0, "denominator: no subscriptions were opened");

        // Subscription registration happens inside the spawned handler; give the
        // whole cohort a bounded window to land before asserting.
        let mut registered = 0usize;
        for _ in 0..200 {
            registered = 0;
            for (_keys, conn, _rx) in &agents {
                registered += conn.subscriptions.lock().await.len();
            }
            if registered == opened {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert_eq!(
            registered, opened,
            "all {opened} subscriptions must be registered before the burst"
        );

        // Phase 2 — concurrent mention burst across every agent.
        let mut published = 0usize;
        let mut tasks = Vec::new();
        for (agent_idx, (keys, conn, _rx)) in agents.iter().enumerate() {
            for event_idx in 0..BURST_EVENTS_PER_AGENT {
                // Mention the *next* agent, so every agent is both author and
                // recipient — the shape of a fleet mention storm.
                let target = &agents[(agent_idx + 1) % BURST_AGENTS].0;
                let event = EventBuilder::new(
                    Kind::TextNote,
                    format!("burst mention a{agent_idx}-e{event_idx}"),
                )
                .tag(nostr::Tag::public_key(target.public_key()))
                .sign_with_keys(keys)
                .expect("sign burst event");
                let raw = serde_json::json!(["EVENT", event]).to_string();
                let conn = Arc::clone(conn);
                let state = Arc::clone(&state);
                tasks.push(tokio::spawn(async move {
                    handle_text_message(raw, conn, state).await;
                }));
                published += 1;
            }
        }
        assert!(published > 0, "denominator: no events were published");
        for task in tasks {
            task.await.expect("burst dispatch task");
        }

        // Let spawned handlers settle.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Phase 3 — assertions with explicit denominators.
        let mut frames_seen = 0usize;
        let mut database_error_closes = Vec::new();
        let mut closed_sub_ids = Vec::new();
        for (_keys, _conn, rx) in agents.iter_mut() {
            for frame in drain_frames(rx) {
                frames_seen += 1;
                if frame[0] == "CLOSED" {
                    let sub_id = frame[1].as_str().unwrap_or_default().to_string();
                    let reason = frame[2].as_str().unwrap_or_default().to_string();
                    if reason.contains("database error") {
                        database_error_closes.push((sub_id.clone(), reason.clone()));
                    }
                    closed_sub_ids.push((sub_id, reason));
                }
            }
        }

        let mut still_open = 0usize;
        for (_keys, conn, _rx) in &agents {
            still_open += conn.subscriptions.lock().await.len();
        }

        eprintln!(
            "burst denominators: agents={BURST_AGENTS} subscriptions_opened={opened} \
events_published={published} frames_observed={frames_seen} \
subscriptions_still_open={still_open} closed_frames={} database_error_closes={}",
            closed_sub_ids.len(),
            database_error_closes.len()
        );

        assert!(
            frames_seen > 0,
            "denominator: the burst produced no observable frames"
        );
        assert!(
            database_error_closes.is_empty(),
            "subscriptions closed with a database error during the mention burst: {database_error_closes:?}"
        );
        assert_eq!(
            still_open, opened,
            "all {opened} subscriptions must stay up through the burst; \
closed frames were: {closed_sub_ids:?}"
        );
    }

    /// Non-vacuity control for the burst assertion.
    ///
    /// The burst asserts "no subscription closed with `error: database error`".
    /// An assertion like that is only worth anything if the condition it forbids
    /// is reachable by the same code path, so this test drives the identical
    /// dispatch against an unreachable database and requires that the forbidden
    /// close *does* appear. If this test ever goes green-by-absence, the burst's
    /// guarantee has silently stopped meaning anything.
    #[tokio::test]
    async fn burst_database_error_close_is_reachable_when_the_database_is_down() {
        let keys = Keys::generate();
        // `test_state` deliberately builds a lazy pool against the configured
        // URL and an unreachable Redis; pointing the pool at a dead port makes
        // every handler DB call fail on connect.
        let mut config = crate::config::Config::from_env().expect("default config loads");
        config.require_relay_membership = false;
        // Admission must succeed so the request actually reaches the database
        // layer; only the database is taken away.
        config.redis_url = burst_redis_url();
        config.database_url = "postgres://buzz:buzz_dev@127.0.0.1:1/buzz".to_string(); // sadscan:disable np.postgres.1 -- unreachable-by-design test DSN
                                                                                       // Short acquire timeout so the unreachable database surfaces as a fast
                                                                                       // acquire failure instead of sitting on sqlx's 30s default.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.db_pool_size)
            .acquire_timeout(std::time::Duration::from_millis(500))
            .connect_lazy(&config.database_url)
            .expect("lazy pg pool");
        let db = buzz_db::Db::from_pool(pool.clone());
        let redis_pool = deadpool_redis::Config::from_url(&config.redis_url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .expect("redis pool");
        let pubsub = Arc::new(
            buzz_pubsub::PubSubManager::new(&config.redis_url, redis_pool.clone())
                .await
                .expect("pubsub manager"),
        );
        let audit = buzz_audit::AuditService::new(pool.clone());
        let auth = buzz_auth::AuthService::new(config.auth.clone());
        let search = buzz_search::SearchService::new(pool.clone());
        let workflow_engine = Arc::new(buzz_workflow::WorkflowEngine::new(
            db.clone(),
            buzz_workflow::WorkflowConfig::default(),
        ));
        let media_storage = buzz_media::MediaStorage::new(&config.media).expect("media storage");
        let (state, _audit_shutdown) = crate::state::AppState::new(
            config,
            db,
            redis_pool,
            audit,
            pubsub,
            auth,
            search,
            workflow_engine,
            nostr::Keys::generate(),
            media_storage,
        );
        let state = Arc::new(state);

        let (conn, mut rx) = burst_conn(
            &keys,
            buzz_core::tenant::CommunityId::from_uuid(Uuid::nil()),
        );
        let raw = serde_json::json!(["REQ", "down-probe", {"kinds": [1], "limit": 1}]).to_string();
        handle_text_message(raw, Arc::clone(&conn), Arc::clone(&state)).await;

        let mut database_error_seen = false;
        let mut frames_seen = 0usize;
        let mut observed: Vec<String> = Vec::new();
        for _ in 0..200 {
            for frame in drain_frames(&mut rx) {
                frames_seen += 1;
                observed.push(frame.to_string());
                if frame[0] == "CLOSED"
                    && frame[2]
                        .as_str()
                        .unwrap_or_default()
                        .contains("database error")
                {
                    database_error_seen = true;
                }
            }
            if database_error_seen {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        eprintln!(
            "burst control denominators: frames_observed={frames_seen} database_error_seen={database_error_seen} observed={observed:?}"
        );
        assert!(
            database_error_seen,
            "the burst's forbidden condition must be reachable; with the database \
down a REQ has to close with `error: database error` (frames seen: {frames_seen})"
        );
    }

    /// Companion to the burst: when the shared admission store is unreachable
    /// the relay must classify the refusal as rate-limiting, never as a
    /// database error. This is the third symptom on the bead
    /// ("rate-limited: shared admission unavailable") and it must stay on the
    /// admission side of the split.
    #[tokio::test]
    async fn admission_unavailable_never_reports_a_database_error() {
        let state = crate::state::tests::test_state().await;
        let (conn, mut rx) = test_conn_with_auth(authenticated_state());

        let raw = serde_json::json!(["REQ", "admission-probe", {"kinds": [1]}]).to_string();
        handle_text_message(raw, Arc::clone(&conn), Arc::clone(&state)).await;

        // The REQ arm spawns; wait briefly for the refusal frame.
        let mut frames = Vec::new();
        for _ in 0..80 {
            frames.extend(drain_frames(&mut rx));
            if !frames.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        assert!(
            !frames.is_empty(),
            "denominator: admission probe produced no frame"
        );
        for frame in &frames {
            if frame[0] == "CLOSED" {
                let reason = frame[2].as_str().unwrap_or_default();
                assert!(
                    !reason.contains("database error"),
                    "admission unavailability must not be reported as a database error, got {reason:?}"
                );
            }
        }
    }

    /// The REQ arm of the same branch still settles on CLOSED.
    #[tokio::test]
    async fn saturated_handler_rejects_a_req_on_the_closed_channel() {
        let state = crate::state::tests::test_state().await;
        let (conn, mut rx) = test_conn_with_auth(AuthState::Failed);

        let permits = state.handler_semaphore.available_permits();
        let _held = Arc::clone(&state.handler_semaphore)
            .acquire_many_owned(permits as u32)
            .await
            .expect("hold every handler permit");

        let raw = serde_json::json!(["REQ", "history-abc", {"kinds": [1]}]).to_string();
        handle_text_message(raw, Arc::clone(&conn), Arc::clone(&state)).await;

        let frame = read_frame(&mut rx);
        assert_eq!(frame[0], "CLOSED");
        assert_eq!(frame[1], "history-abc");
    }

    /// COUNT refusals follow NIP-45 and close the named query.
    #[tokio::test]
    async fn saturated_handler_rejects_a_count_on_the_closed_channel() {
        let state = crate::state::tests::test_state().await;
        let (conn, mut rx) = test_conn_with_auth(AuthState::Failed);

        let permits = state.handler_semaphore.available_permits();
        let _held = Arc::clone(&state.handler_semaphore)
            .acquire_many_owned(permits as u32)
            .await
            .expect("hold every handler permit");

        let raw = serde_json::json!(["COUNT", "count-abc", {"kinds": [1]}]).to_string();
        handle_text_message(raw, Arc::clone(&conn), Arc::clone(&state)).await;

        let frame = read_frame(&mut rx);
        assert_eq!(frame[0], "CLOSED");
        assert_eq!(frame[1], "count-abc");
        assert_eq!(frame[2], "rate-limited: too many concurrent requests");
    }

    #[derive(Debug, Default)]
    struct MockSinkState {
        messages: Vec<WsMessage>,
        flush_count: usize,
        fail_after_flushes: Option<usize>,
    }

    #[derive(Debug, Clone)]
    struct MockSink {
        state: Arc<Mutex<MockSinkState>>,
    }

    impl MockSink {
        fn new(fail_after_flushes: Option<usize>) -> (Self, Arc<Mutex<MockSinkState>>) {
            let state = Arc::new(Mutex::new(MockSinkState {
                fail_after_flushes,
                ..MockSinkState::default()
            }));
            (
                Self {
                    state: Arc::clone(&state),
                },
                state,
            )
        }
    }

    impl Sink<WsMessage> for MockSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
            self.state
                .lock()
                .expect("mock sink poisoned")
                .messages
                .push(item);
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("mock sink poisoned");
            state.flush_count += 1;
            if state
                .fail_after_flushes
                .is_some_and(|limit| state.flush_count >= limit)
            {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "mock flush failure",
                )));
            }
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            self.poll_flush(cx)
        }
    }

    fn ordinary_disconnect_reason() -> watch::Receiver<Option<CommunityDisconnectReason>> {
        let (_tx, rx) = watch::channel(None);
        rx
    }

    fn deleted_community_disconnect_reason() -> watch::Receiver<Option<CommunityDisconnectReason>> {
        let (tx, rx) = watch::channel(None);
        tx.send_replace(Some(CommunityDisconnectReason::CommunityDeleted));
        rx
    }

    fn text_payloads(messages: &[WsMessage]) -> Vec<String> {
        messages
            .iter()
            .map(|msg| match msg {
                WsMessage::Text(text) => text.to_string(),
                other => panic!("unexpected websocket message in test: {other:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn send_loop_batches_queued_data_frames_into_one_flush() {
        let (data_tx, data_rx) = mpsc::channel(MAX_WS_SEND_BATCH);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        for i in 0..5 {
            data_tx
                .send(WsMessage::Text(format!("data-{i}").into()))
                .await
                .expect("queue data frame");
        }

        let (sink, state) = MockSink::new(Some(1));
        let (_restart_tx, restart_rx) = mpsc::channel(1);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            CancellationToken::new(),
            ordinary_disconnect_reason(),
        )
        .await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 1);
        assert_eq!(
            text_payloads(&state.messages),
            vec!["data-0", "data-1", "data-2", "data-3", "data-4"]
        );
    }

    #[tokio::test]
    async fn send_loop_batch_one_preserves_single_frame_flush_behavior() {
        let (data_tx, data_rx) = mpsc::channel(1);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        data_tx
            .send(WsMessage::Text("single".into()))
            .await
            .expect("queue data frame");

        let (sink, state) = MockSink::new(Some(1));
        let (_restart_tx, restart_rx) = mpsc::channel(1);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            CancellationToken::new(),
            ordinary_disconnect_reason(),
        )
        .await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 1);
        assert_eq!(text_payloads(&state.messages), vec!["single"]);
    }

    #[tokio::test]
    async fn send_loop_drains_control_before_batched_data_without_reordering() {
        let (data_tx, data_rx) = mpsc::channel(MAX_WS_SEND_BATCH);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
        data_tx
            .send(WsMessage::Text("data-0".into()))
            .await
            .expect("queue data frame");
        data_tx
            .send(WsMessage::Text("data-1".into()))
            .await
            .expect("queue data frame");
        ctrl_tx
            .send(WsMessage::Text("control".into()))
            .await
            .expect("queue control frame");

        let (sink, state) = MockSink::new(Some(2));
        let (_restart_tx, restart_rx) = mpsc::channel(1);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            CancellationToken::new(),
            ordinary_disconnect_reason(),
        )
        .await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 2);
        assert_eq!(
            text_payloads(&state.messages),
            vec!["control", "data-0", "data-1"]
        );
    }

    #[tokio::test]
    async fn send_loop_acknowledges_restart_after_flushing_exactly_one_1012() {
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        let (restart_tx, restart_rx) = mpsc::channel(1);
        let (flushed_tx, flushed_rx) = tokio::sync::oneshot::channel();
        restart_tx
            .send(RestartClose {
                flushed: flushed_tx,
            })
            .await
            .expect("queue restart close");

        let (sink, state) = MockSink::new(None);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            CancellationToken::new(),
            ordinary_disconnect_reason(),
        )
        .await;

        assert_eq!(flushed_rx.await, Ok(true));
        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 1, "ack follows the close flush");
        assert_eq!(state.messages.len(), 1, "writer exits after restart close");
        match &state.messages[0] {
            WsMessage::Close(Some(close)) => {
                assert_eq!(close.code, axum::extract::ws::close_code::RESTART);
                assert_eq!(close.reason.as_str(), "relay restarting");
            }
            other => panic!("expected one 1012 restart close, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_loop_reports_restart_flush_failure() {
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        let (restart_tx, restart_rx) = mpsc::channel(1);
        let (flushed_tx, flushed_rx) = tokio::sync::oneshot::channel();
        restart_tx
            .send(RestartClose {
                flushed: flushed_tx,
            })
            .await
            .expect("queue restart close");

        let (sink, state) = MockSink::new(Some(1));
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            CancellationToken::new(),
            ordinary_disconnect_reason(),
        )
        .await;

        assert_eq!(flushed_rx.await, Ok(false));
        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 1);
        assert_eq!(state.messages.len(), 1, "no fallback close is appended");
    }

    #[tokio::test]
    async fn send_loop_sends_policy_close_when_community_is_deleted() {
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        let (_restart_tx, restart_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let (sink, state) = MockSink::new(None);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            cancel,
            deleted_community_disconnect_reason(),
        )
        .await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.messages.len(), 1);
        match &state.messages[0] {
            WsMessage::Close(Some(close)) => {
                assert_eq!(close.code, axum::extract::ws::close_code::POLICY);
                assert_eq!(close.reason.as_str(), "community deleted");
            }
            other => panic!("expected one 1008 deletion close, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_loop_sends_bare_close_for_ordinary_cancellation() {
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        let (_restart_tx, restart_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let (sink, state) = MockSink::new(None);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            cancel,
            ordinary_disconnect_reason(),
        )
        .await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.messages.as_slice(), [WsMessage::Close(None)]);
    }

    #[tokio::test]
    async fn send_loop_flushes_queued_control_before_close_on_cancel() {
        // A ban disconnect queues its `OK false "blocked: …"` reason frame on
        // the control channel and then cancels the token (B3). The biased
        // select polls the cancel branch first, so the reason frame would be
        // stranded unless the cancel branch drains ctrl before emitting Close.
        // This test exercises `send_loop_inner` end-to-end to prove the reason
        // frame reaches the client, in order, ahead of the Close.
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
        ctrl_tx
            .send(WsMessage::Text("blocked: you are banned".into()))
            .await
            .expect("queue ban reason frame");

        let cancel = CancellationToken::new();
        cancel.cancel();

        let (sink, state) = MockSink::new(None);
        let (_restart_tx, restart_rx) = mpsc::channel(1);
        send_loop_inner(
            sink,
            data_rx,
            ctrl_rx,
            restart_rx,
            cancel,
            ordinary_disconnect_reason(),
        )
        .await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(
            state.messages.len(),
            2,
            "reason frame then Close, nothing else"
        );
        match &state.messages[0] {
            WsMessage::Text(text) => {
                assert_eq!(text.as_str(), "blocked: you are banned")
            }
            other => panic!("expected the ban reason frame first, got {other:?}"),
        }
        assert!(
            matches!(state.messages[1], WsMessage::Close(None)),
            "ordinary cancellation retains the bare Close after the reason frame"
        );
    }
}
