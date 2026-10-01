//! Shared test-only helpers, `pub` (not `#[cfg(test)]`) so other workspace crates'
//! own tests can depend on this crate and reuse them.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A local mosquitto instance on an ephemeral port, for exercising the real MQTT
/// wire protocol rather than mocking rumqttc. Environments without a `mosquitto`
/// binary should skip tests using this instead of failing the suite.
pub struct TestBroker {
    child: Child,
    port: u16,
}

impl TestBroker {
    pub fn start() -> Option<Self> {
        let port = {
            let listener = TcpListener::bind("127.0.0.1:0").ok()?;
            listener.local_addr().ok()?.port()
        };
        let child = match Command::new("mosquitto")
            .args(["-p", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(_) => {
                eprintln!("skipping mqtt integration test: mosquitto not found on PATH");
                return None;
            }
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            if Instant::now() > deadline {
                panic!("mosquitto did not start listening on {port} in time");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Some(Self { child, port })
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
