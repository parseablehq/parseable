use std::{collections::HashMap, sync::Arc, time::Duration};

use actix_web::{HttpRequest, rt::time::interval};
use actix_web_lab::{
    sse::{self, Sse},
    util::InfallibleStream,
};
use futures_util::future;

use itertools::Itertools;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use ulid::Ulid;

use crate::{
    alerts::AlertState, rbac::map::SessionKey, utils::actix::extract_session_key_from_req,
};

pub static SSE_HANDLER: Lazy<Arc<Broadcaster>> = Lazy::new(Broadcaster::create);

pub struct Broadcaster {
    inner: RwLock<BroadcasterInner>,
}

#[derive(Debug, Clone, Default)]
struct BroadcasterInner {
    // hashmap to map sse session to prism ui session
    clients: HashMap<Ulid, Vec<mpsc::Sender<sse::Event>>>,
}

impl Broadcaster {
    /// Constructs new broadcaster and spawns ping loop.
    pub fn create() -> Arc<Self> {
        let this = Arc::new(Broadcaster {
            inner: RwLock::new(BroadcasterInner::default()),
        });

        Broadcaster::spawn_ping(Arc::clone(&this));

        this
    }

    /// Pings clients every 10 seconds to see if they are alive and remove them from the broadcast
    /// list if not.
    fn spawn_ping(this: Arc<Self>) {
        actix_web::rt::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));

            loop {
                interval.tick().await;
                this.remove_stale_clients().await;
            }
        });
    }

    /// Removes all non-responsive clients from broadcast list.
    async fn remove_stale_clients(&self) {
        let sse_inner = self.inner.read().await.clients.clone();

        let mut ok_sessions = HashMap::new();

        for (session, clients) in sse_inner.iter() {
            let mut ok_clients = Vec::new();
            for client in clients {
                if client
                    .send(sse::Event::Comment("ping".into()))
                    .await
                    .is_ok()
                {
                    ok_clients.push(client.clone())
                }
            }
            ok_sessions.insert(*session, ok_clients);
        }

        self.inner.write().await.clients = ok_sessions;
    }

    /// Registers client with broadcaster, returning an SSE response body.
    pub async fn new_client(
        &self,
        session: &Ulid,
    ) -> Sse<InfallibleStream<ReceiverStream<sse::Event>>> {
        let (tx, rx) = mpsc::channel(10);

        let _ = tx.send(sse::Data::new("connected").into()).await;

        self.inner
            .write()
            .await
            .clients
            .entry(*session)
            .or_default()
            .push(tx);

        Sse::from_infallible_receiver(rx)
    }

    pub async fn fetch_sessions(&self) -> Vec<Ulid> {
        self.inner
            .read()
            .await
            .clients
            .keys()
            .cloned()
            .collect_vec()
    }

    /// Broadcasts `msg`
    ///
    /// If sessions is None, then broadcast to all
    pub async fn broadcast(&self, msg: &str, sessions: Option<&[Ulid]>) {
        let clients = self.inner.read().await.clients.clone();
        if clients.is_empty() {
            return;
        }

        let send_futures = if let Some(sessions) = sessions {
            let mut futures = vec![];
            for (session, clients) in clients.iter() {
                if sessions.contains(session) {
                    clients
                        .iter()
                        .for_each(|client| futures.push(client.send(sse::Data::new(msg).into())));
                }
            }
            futures
        } else {
            // broadcast
            let mut futures = vec![];
            for (_, clients) in clients.iter() {
                clients
                    .iter()
                    .for_each(|client| futures.push(client.send(sse::Data::new(msg).into())));
            }
            futures
        };

        // try to send to all clients, ignoring failures
        // disconnected clients will get swept up by `remove_stale_clients`
        let _ = future::join_all(send_futures).await;
    }
}

pub async fn register_sse_client(
    req: HttpRequest,
) -> Result<Sse<InfallibleStream<ReceiverStream<sse::Event>>>, actix_web::Error> {
    let session = extract_session_key_from_req(&req).unwrap();
    let sessionid = match session {
        SessionKey::SessionId(ulid) => ulid,
        _ => {
            return Err(actix_web::error::ErrorBadRequest(
                "SSE requires session-based authentication, not BasicAuth",
            ));
        }
    };
    Ok(SSE_HANDLER.new_client(&sessionid).await)
}

/// Struct to define the messages being sent using SSE
#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SSEEvent {
    pub criticality: Criticality,
    pub message: Message,
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Message {
    AlertEvent(SSEAlertInfo),
    ControlPlaneEvent(ControlPlaneEvent),
    Consent(Consent),
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Criticality {
    Info,
    Warn,
    Error,
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SSEAlertInfo {
    pub id: Ulid,
    pub state: AlertState,
    pub name: String,
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneEvent {
    message: String,
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Consent {
    given: bool,
}
