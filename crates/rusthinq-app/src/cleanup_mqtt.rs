//! Dedicated MQTT 3.1.1 connection for owned retained values and acknowledged deletion.
use crate::retained_cleanup::Ledger;
use rusthinq_protocol::mqtt;
use rusthinq_server::retained::Tombstone;
use std::{io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

pub struct Session<S> {
    stream: S,
    deadline: Duration,
    next: u16,
    usable: bool,
}
impl<S: AsyncRead + AsyncWrite + Unpin> Session<S> {
    /// Caller supplies a connected, authenticated transport (TLS if required).
    /// This connection is exclusive: no subscriptions or other in-flight publishers.
    pub async fn connect(mut stream: S, client: &str, deadline: Duration) -> io::Result<Self> {
        if deadline.is_zero()
            || client.is_empty()
            || client.len() > 1024
            || client.chars().any(char::is_control)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid cleanup MQTT configuration",
            ));
        }
        let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0];
        body.extend_from_slice(&(client.len() as u16).to_be_bytes());
        body.extend_from_slice(client.as_bytes());
        let packet = mqtt::frame(0x10, &body, 2048).map_err(|_| invalid("CONNECT exceeded"))?;
        timeout(deadline, async {
            stream.write_all(&packet).await?;
            stream.flush().await?;
            let mut ack = [0; 4];
            stream.read_exact(&mut ack).await?;
            if ack != [0x20, 2, 0, 0] {
                return Err(invalid("invalid or rejected CONNACK"));
            }
            Ok(())
        })
        .await
        .map_err(|_| timed_out())??;
        Ok(Self {
            stream,
            deadline,
            next: 0,
            usable: true,
        })
    }
    async fn delete(&mut self, deletion: &Tombstone) -> io::Result<()> {
        self.retained_exchange(&deletion.topic, &[]).await
    }
    async fn retained_exchange(&mut self, topic: &str, payload: &[u8]) -> io::Result<()> {
        if !self.usable {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "retained session requires reconnect",
            ));
        }
        let id = self.next.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "packet identifiers exhausted; reconnect",
            )
        })?;
        self.next = id;
        self.usable = false;
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(payload);
        let frame =
            mqtt::frame(0x33, &body, 1_052_672).map_err(|_| invalid("retained packet exceeded"))?;
        timeout(self.deadline, async {
            self.stream.write_all(&frame).await?;
            self.stream.flush().await?;
            let mut ack = [0; 4];
            self.stream.read_exact(&mut ack).await?;
            if ack != [0x40, 2, (id >> 8) as u8, id as u8] {
                return Err(invalid("unexpected PUBACK"));
            }
            Ok(())
        })
        .await
        .map_err(|_| timed_out())??;
        self.usable = true;
        Ok(())
    }
    /// Register the exact removal route durably before any retained value bytes.
    /// Publication PUBACK never removes inventory; only acknowledged deletion does.
    pub async fn publish_owned_retained(
        &mut self,
        ledger: Ledger,
        owner: String,
        topic: String,
        payload: &[u8],
    ) -> io::Result<Ledger> {
        if payload.is_empty() || payload.len() > 1_048_576 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "use deletion for empty payload; retained value limit is 1 MiB",
            ));
        }
        let deletion = Tombstone { owner, topic };
        let (ledger, deletion) = tokio::task::spawn_blocking(move || {
            let mut ledger = ledger;
            ledger.enqueue(std::slice::from_ref(&deletion))?;
            Ok::<_, io::Error>((ledger, deletion))
        })
        .await
        .map_err(io::Error::other)??;
        self.retained_exchange(&deletion.topic, payload).await?;
        Ok(ledger)
    }

    /// Clean only this owner's topics; preserve other device/adapter inventories.
    pub async fn remove_owner(
        &mut self,
        mut ledger: Ledger,
        owner: &str,
    ) -> io::Result<(Ledger, usize)> {
        let topics: Vec<_> = ledger
            .pending()
            .into_iter()
            .filter(|item| item.owner == owner)
            .map(|item| item.topic)
            .collect();
        let mut completed = 0;
        for topic in topics {
            ledger = self.delete_one(ledger, topic).await?;
            completed += 1;
        }
        Ok((ledger, completed))
    }
    /// Owns the ledger during the operation. On error drop/reopen it from disk;
    /// durable work survives. Blocking commits run off protocol I/O tasks.
    pub async fn delete_one(&mut self, ledger: Ledger, topic: String) -> io::Result<Ledger> {
        let (ledger, attempt) = tokio::task::spawn_blocking(move || {
            let mut ledger = ledger;
            let attempt = ledger.begin(&topic)?;
            Ok::<_, io::Error>((ledger, attempt))
        })
        .await
        .map_err(io::Error::other)??;
        self.delete(&attempt.deletion).await?;
        tokio::task::spawn_blocking(move || {
            let mut ledger = ledger;
            if !ledger.complete(&attempt, true)? {
                return Err(invalid("stale cleanup completion"));
            }
            Ok(ledger)
        })
        .await
        .map_err(io::Error::other)?
    }
    /// Drain recovered/startup/removal work sequentially on this exclusive connection.
    /// Each PUBACK is durably committed before the next deletion is sent.
    /// On failure, reopen the ledger and reconnect: confirmed work stays removed,
    /// while the failed and unsent topics remain recoverable. No retry task is spawned.
    pub async fn drain(&mut self, mut ledger: Ledger) -> io::Result<(Ledger, usize)> {
        let pending = ledger.pending();
        let mut completed = 0;
        for deletion in pending {
            ledger = self.delete_one(ledger, deletion.topic).await?;
            completed += 1;
        }
        Ok((ledger, completed))
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "cleanup MQTT exchange timed out")
}
