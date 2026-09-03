//! Subprocess helper (port of bridge/util.ts).

use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_STDERR_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Default)]
pub struct SubprocessOptions {
    pub timeout_ms: Option<u64>,
    pub max_output_bytes: Option<usize>,
    pub max_stderr_bytes: Option<usize>,
}

#[derive(Debug)]
pub struct SubprocessError(pub String);

impl std::fmt::Display for SubprocessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SubprocessError {}

fn sanitize_diagnostic(stderr: &[u8]) -> String {
    if stderr.is_empty() {
        return String::new();
    }
    let text: String = String::from_utf8_lossy(stderr)
        .chars()
        .map(|c| {
            let u = c as u32;
            if (u <= 0x08) || u == 0x0b || u == 0x0c || (0x0e..=0x1f).contains(&u) || u == 0x7f {
                '?'
            } else {
                c
            }
        })
        .collect();
    let text = text.trim();
    if text.is_empty() {
        String::new()
    } else {
        format!(": {text}")
    }
}

/// Run a subprocess, writing `stdin` and returning stdout on success.
pub async fn subprocess(
    command: &str,
    args: &[&str],
    stdin: &str,
    options: SubprocessOptions,
) -> Result<String, SubprocessError> {
    let timeout_ms = options.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    let max_output_bytes = options.max_output_bytes.unwrap_or(DEFAULT_MAX_OUTPUT_BYTES);
    let max_stderr_bytes = options.max_stderr_bytes.unwrap_or(DEFAULT_MAX_STDERR_BYTES);

    let mut child = match Command::new(command)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return Err(SubprocessError(format!(
                "Failed to start subprocess {command}: {e}"
            )));
        }
    };

    if let Some(mut stdin_pipe) = child.stdin.take() {
        if let Err(e) = stdin_pipe.write_all(stdin.as_bytes()).await {
            let _ = child.kill().await;
            return Err(SubprocessError(format!(
                "Subprocess {command} stdin failed: {e}"
            )));
        }
        drop(stdin_pipe);
    }

    let mut stdout = child.stdout.take().expect("stdout piped");
    let mut stderr = child.stderr.take().expect("stderr piped");

    let out_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match stdout.read(&mut chunk).await {
                Ok(0) => break Ok(buf),
                Ok(n) => {
                    if buf.len() + n > max_output_bytes {
                        break Err(format!(
                            "Subprocess exceeded {max_output_bytes} bytes of stdout"
                        ));
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => break Err(format!("stdout read error: {e}")),
            }
        }
    });

    let err_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            match stderr.read(&mut chunk).await {
                Ok(0) => break buf,
                Ok(n) => {
                    let remaining = max_stderr_bytes.saturating_sub(buf.len());
                    if remaining == 0 {
                        continue;
                    }
                    let take = n.min(remaining);
                    buf.extend_from_slice(&chunk[..take]);
                }
                Err(_) => break buf,
            }
        }
    });

    let wait_status = timeout(Duration::from_millis(timeout_ms), child.wait()).await;

    match wait_status {
        Err(_) | Ok(Err(_)) => {
            let _ = child.kill().await;
            let err_bytes = err_task.await.unwrap_or_default();
            let _ = out_task.await;
            Err(SubprocessError(format!(
                "Subprocess {command} timed out after {timeout_ms}ms{}",
                sanitize_diagnostic(&err_bytes)
            )))
        }
        Ok(Ok(status)) => {
            let out_res = out_task
                .await
                .map_err(|e| SubprocessError(format!("stdout join: {e}")))?;
            let err_bytes = err_task.await.unwrap_or_default();
            let diag = sanitize_diagnostic(&err_bytes);
            match out_res {
                Err(msg) => {
                    // msg like "Subprocess exceeded N bytes of stdout"
                    let msg = if let Some(rest) = msg.strip_prefix("Subprocess ") {
                        format!("Subprocess {command} {rest}")
                    } else {
                        format!("Subprocess {command} {msg}")
                    };
                    Err(SubprocessError(format!("{msg}{diag}")))
                }
                Ok(stdout_bytes) => {
                    if let Some(sig) = exit_signal(&status) {
                        let name = signal_name(sig);
                        return Err(SubprocessError(format!(
                            "Subprocess {command} terminated by {name}{diag}"
                        )));
                    }
                    let code = status.code().unwrap_or(-1);
                    if code != 0 {
                        return Err(SubprocessError(format!(
                            "Subprocess {command} exited with code {code}{diag}"
                        )));
                    }
                    Ok(String::from_utf8_lossy(&stdout_bytes).into_owned())
                }
            }
        }
    }
}

#[cfg(unix)]
fn signal_name(sig: i32) -> String {
    match sig {
        15 => "SIGTERM".into(),
        9 => "SIGKILL".into(),
        2 => "SIGINT".into(),
        n => format!("signal {n}"),
    }
}

#[cfg(not(unix))]
fn signal_name(sig: i32) -> String {
    format!("signal {sig}")
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subprocess_returns_stdout_only_for_successful_exit() {
        let out = subprocess("sh", &["-c", "printf ok"], "", SubprocessOptions::default())
            .await
            .unwrap();
        assert_eq!(out, "ok");
    }

    #[tokio::test]
    async fn subprocess_rejects_nonzero_exits_with_bounded_sanitized_stderr() {
        let err = subprocess(
            "sh",
            &["-c", "printf 'bad\\0detail' >&2; exit 7"],
            "",
            SubprocessOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(err.0.contains("exited with code 7"), "msg={}", err.0);
        assert!(err.0.contains("bad?detail"), "msg={}", err.0);
    }

    #[tokio::test]
    async fn subprocess_rejects_signals_and_spawn_failures() {
        let err = subprocess(
            "sh",
            &["-c", "kill -TERM $$"],
            "",
            SubprocessOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(err.0.contains("terminated by SIGTERM"), "msg={}", err.0);

        let err = subprocess(
            "/definitely/not/a/real/executable",
            &[],
            "",
            SubprocessOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(
            err.0.contains("Failed to start subprocess"),
            "msg={}",
            err.0
        );
    }

    #[tokio::test]
    async fn subprocess_enforces_its_timeout() {
        // Warm spawn for timing baseline (mirrors TS test).
        let started = std::time::Instant::now();
        let _ = subprocess(
            "sh",
            &["-c", "printf warm >&2"],
            "",
            SubprocessOptions::default(),
        )
        .await;
        let timeout_ms = (started.elapsed().as_millis() as u64).max(1) * 4 + 50;

        let err = subprocess(
            "sh",
            &[
                "-c",
                "printf 'timed detail' >&2; while true; do sleep 1; done",
            ],
            "",
            SubprocessOptions {
                timeout_ms: Some(timeout_ms),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        let re = format!("timed out after {timeout_ms}ms: timed detail");
        assert!(err.0.contains(&re), "msg={}", err.0);
    }

    #[tokio::test]
    async fn subprocess_rejects_oversized_stdout_and_bounds_stderr() {
        let err = subprocess(
            "sh",
            &[
                "-c",
                "python3 -c 'import sys; sys.stdout.write(\"x\"*10000)'",
            ],
            "",
            SubprocessOptions {
                max_output_bytes: Some(1024),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            err.0.contains("exceeded 1024 bytes of stdout"),
            "msg={}",
            err.0
        );

        let err = subprocess(
            "sh",
            &[
                "-c",
                "python3 -c 'import sys; sys.stderr.write(\"x\"*10000)'; exit 1",
            ],
            "",
            SubprocessOptions {
                max_stderr_bytes: Some(128),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(err.0.contains("exited with code 1"), "msg={}", err.0);
        assert!(err.0.len() < 256, "msg len={}", err.0.len());
    }
}
