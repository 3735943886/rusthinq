use super::*;
#[derive(Clone)]
pub(super) enum TransportHandle {
    ThinQ1(ServerHandle),
    ThinQ2(rusthinq_server::mqtt::Handle),
    Mixed {
        server: ServerHandle,
        broker: rusthinq_server::mqtt::Handle,
    },
}
impl TransportHandle {
    pub(super) fn protocol(
        &self,
        id: &SessionId,
    ) -> Result<rusthinq_server::Protocol, rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(server) | Self::Mixed { server, .. } => server.protocol(id),
            Self::ThinQ2(broker) => {
                if broker.snapshot().contains(id) {
                    Ok(rusthinq_server::Protocol::ThinQ2)
                } else {
                    Err(rusthinq_server::Reject::StaleSession)
                }
            }
        }
    }
    pub(super) fn send(
        &self,
        session: &SessionId,
        payload: &[u8],
    ) -> Result<rusthinq_server::Receipt, rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) => handle.send(session, payload),
            Self::ThinQ2(handle) => handle.send(session, payload),
            Self::Mixed { server, broker } => match server.protocol(session)? {
                rusthinq_server::Protocol::ThinQ1 => server.send(session, payload),
                rusthinq_server::Protocol::ThinQ2 => broker.send(session, payload),
            },
        }
    }
    pub(super) fn subscribe(&self) -> broadcast::Receiver<TransportEvent> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.subscribe(),
            Self::ThinQ2(handle) => handle.subscribe(),
        }
    }
    pub(super) fn snapshot(&self) -> Vec<SessionId> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.snapshot(),
            Self::ThinQ2(handle) => handle.snapshot(),
        }
    }
    pub(super) fn close(&self, session: &SessionId) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.close(session),
            Self::ThinQ2(handle) => handle.close(session),
        }
    }
    pub(super) async fn close_and_wait(
        &self,
        session: &SessionId,
    ) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => {
                handle.close_and_wait(session).await
            }
            Self::ThinQ2(handle) => handle.close_and_wait(session).await,
        }
    }
    pub(super) fn generation_budget(&self) -> (u64, u64) {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => handle.generation_budget(),
            Self::ThinQ2(handle) => handle.generation_budget(),
        }
    }
    pub(super) fn extend_generations(
        &self,
        floor: u64,
        ceiling: u64,
    ) -> Result<(), rusthinq_server::Reject> {
        match self {
            Self::ThinQ1(handle) | Self::Mixed { server: handle, .. } => {
                handle.extend_generations(floor, ceiling)
            }
            Self::ThinQ2(handle) => handle.extend_generations(floor, ceiling),
        }
    }
}
