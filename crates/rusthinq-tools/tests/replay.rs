use rusthinq_tools::{Client, replay};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[test]
fn plan_preserves_receive_order_and_refuses_loss_or_invalid_hex_before_submission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capture.jsonl");
    std::fs::write(&path,"{\"k\":\"note\",\"text\":\"legacy\"}\n{\"k\":\"rx\",\"hex\":\"AABB\"}\n{\"k\":\"tx\",\"hex\":\"0011\"}\n{\"k\":\"rx\",\"type\":\"clip\",\"hex\":\"{}\"}\n{\"k\":\"rx\",\"type\":\"packet\",\"hex\":\"0022\"}\n").unwrap();
    assert_eq!(replay::plan(&path).unwrap(), vec!["aabb", "0022"]);
    std::fs::write(
        &path,
        "{\"k\":\"rx\",\"hex\":\"0011\"}\n{\"k\":\"lost\",\"events\":1}\n",
    )
    .unwrap();
    assert!(replay::plan(&path).is_err());
    std::fs::write(&path, "{\"k\":\"rx\",\"hex\":\"zz\"}\n").unwrap();
    assert!(replay::plan(&path).is_err());
}
#[tokio::test]
async fn replay_keeps_captured_scope_and_stops_at_first_unconfirmed_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capture.jsonl");
    std::fs::write(&path,"{\"k\":\"rx\",\"hex\":\"0011\"}\n{\"k\":\"rx\",\"hex\":\"0022\"}\n{\"k\":\"rx\",\"hex\":\"0033\"}\n").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for index in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(socket.read_u8().await.unwrap());
                assert!(headers.len() < 8192);
            }
            let headers = String::from_utf8(headers).unwrap();
            let (status, response) = if index == 0 {
                assert!(headers.starts_with("GET /api/devices "));
                ("200 OK",json!({"devices":{"d":{"incarnation":"7","generation":"9","scriptGeneration":"11"}}}).to_string())
            } else {
                assert!(headers.starts_with("POST /api/devices/d/inject "));
                let size = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|n| n.parse::<usize>().ok())
                    })
                    .unwrap();
                let mut bytes = vec![0; size];
                socket.read_exact(&mut bytes).await.unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["incarnation"], "7");
                assert_eq!(body["generation"], "9");
                assert_eq!(body["direction"], "fromDevice");
                assert_eq!(body["hex"], if index == 1 { "0011" } else { "0022" });
                if index == 1 {
                    ("200 OK", "{\"injected\":true}".into())
                } else {
                    ("409 Conflict", "{\"error\":\"stale session\"}".into())
                }
            };
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        }
    });
    let client = Client::new(&format!("http://{address}/"), None, None).unwrap();
    assert_eq!(
        replay::replay(&client, "d", &path, false)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        replay::replay(&client, "d", &path, true),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("after 1 submissions"));
    server.await.unwrap();
}
