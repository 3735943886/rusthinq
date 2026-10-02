use rusthinq_bridge::session::{self, Event, Identity, MqttConfig, Uplink};
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, duplex},
    sync::{mpsc, oneshot, watch},
    time::timeout,
};
fn identity() -> Identity {
    Identity {
        device: "d".into(),
        model: "MODEL".into(),
    }
}
async fn rti<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    let size = stream.read_u32().await.unwrap();
    let mut bytes = vec![0; size as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    bytes
}
async fn mqtt<S: AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    let mut bytes = vec![stream.read_u8().await.unwrap()];
    loop {
        let byte = stream.read_u8().await.unwrap();
        bytes.push(byte);
        if byte & 128 == 0 {
            break;
        }
    }
    let size = rusthinq_protocol::mqtt::length(&bytes, 1_004_096)
        .unwrap()
        .unwrap();
    bytes.resize(size, 0);
    let header = bytes.iter().skip(1).position(|b| b & 128 == 0).unwrap() + 2;
    stream.read_exact(&mut bytes[header..]).await.unwrap();
    bytes
}
#[tokio::test]
async fn rti_start_stop_latest_status_and_delivered_command_ack() {
    let (client, mut peer) = duplex(8192);
    let (uplink, rx) = mpsc::channel(4);
    let (events, mut received) = mpsc::channel(4);
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(session::thinq1(client, identity(), rx, events, stopped));
    assert!(matches!(received.recv().await, Some(Event::Ready)));
    let alive: Value = serde_json::from_slice(&rti(&mut peer).await).unwrap();
    assert_eq!(alive["Body"]["Cmd"], "Alive");
    let status = json!({"Body":{"Format":"B64","Data":"AQI="}})
        .to_string()
        .into_bytes();
    let (result, reply) = oneshot::channel();
    uplink
        .send(Uplink {
            payload: status,
            result,
        })
        .await
        .unwrap();
    assert_eq!(
        reply.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let start = rusthinq_protocol::thinq1::encode(br#"{"Body":{"CmdOpt":"Start"}}"#, 1024).unwrap();
    for byte in start {
        peer.write_all(&[byte]).await.unwrap();
    }
    let status: Value = serde_json::from_slice(&rti(&mut peer).await).unwrap();
    assert_eq!(status["Body"]["Data"], "AQI=");
    let command=br#"{ "Header":{"x-lgedm-deviceId":"d"},"Body":{"CmdWId":"remote","Cmd":"Ctrl","unknown":true} }"#;
    peer.write_all(&rusthinq_protocol::thinq1::encode(command, 1024).unwrap())
        .await
        .unwrap();
    let Event::Downlink { payload, result } = received.recv().await.unwrap() else {
        panic!()
    };
    assert_eq!(payload, command);
    assert!(
        timeout(Duration::from_millis(20), peer.read_u8())
            .await
            .is_err()
    );
    result.send(true).unwrap();
    let ack: Value = serde_json::from_slice(&rti(&mut peer).await).unwrap();
    assert_eq!(ack["Body"]["CmdWId"], "remote");
    assert_eq!(ack["Body"]["ReturnCode"], "0000");
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert_eq!(
        peer.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
}
#[tokio::test]
async fn mqtt_preserves_cloud_mid_and_correlations_and_orders_uplinks() {
    let (client, mut peer) = duplex(8192);
    let (uplink, rx) = mpsc::channel(4);
    let (events, mut received) = mpsc::channel(4);
    let (stop, stopped) = watch::channel(false);
    let config = MqttConfig {
        identity: identity(),
        publish: "up".into(),
        provisioning: "prov".into(),
        subscribe: "down".into(),
        deploy: json!({"did":"d","kind":"MODEL","mid":7,"data":{"appInfo":{"protocolVer":"7"},"platformInfo":{"version":"real"}}}),
    };
    let task = tokio::spawn(session::thinq2(client, config, rx, events, stopped));
    assert_eq!(mqtt(&mut peer).await[0], 0x10);
    peer.write_all(&[0x20, 2, 0, 0]).await.unwrap();
    assert_eq!(mqtt(&mut peer).await[0], 0x82);
    peer.write_all(&[0x90, 3, 0, 1, 1]).await.unwrap();
    let pre = mqtt(&mut peer).await;
    let rusthinq_protocol::mqtt::Packet::Publish { payload, id, .. } =
        rusthinq_protocol::mqtt::decode(&pre, 1_004_096).unwrap()
    else {
        panic!()
    };
    assert_eq!(id, Some(2));
    let pre: Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(pre["cmd"], "preDeploy");
    assert_eq!(pre["data"]["appInfo"]["protocolVer"], "7");
    peer.write_all(&[0x40, 2, 0, 2]).await.unwrap();
    peer.write_all(
        &rusthinq_protocol::mqtt::publish(
            "down",
            br#"{"did":"d","mid":8,"cmd":"completeProvisioning"}"#,
            1024,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let complete = mqtt(&mut peer).await;
    assert_eq!(complete[0], 0x30);
    assert!(matches!(received.recv().await, Some(Event::Ready)));
    let payload=br#"{ "did":"d","mid":999,"cmd":"reqUniversalCtrl","data":{"messageId":"correlation","unknown":true} }"#;
    peer.write_all(&rusthinq_protocol::mqtt::publish("down", payload, 1024).unwrap())
        .await
        .unwrap();
    let Event::Downlink {
        payload: actual,
        result,
    } = received.recv().await.unwrap()
    else {
        panic!()
    };
    assert_eq!(actual, payload);
    result.send(true).unwrap();
    for i in 0..3 {
        let (result, reply) = oneshot::channel();
        uplink.send(Uplink{payload:json!({"did":"d","mid":1,"cmd":"respUniversalCtrl","data":{"messageId":"correlation","sequence":i}}).to_string().into_bytes(),result}).await.unwrap();
        let packet = mqtt(&mut peer).await;
        assert_eq!(packet[0], 0x30);
        let rusthinq_protocol::mqtt::Packet::Publish { payload, .. } =
            rusthinq_protocol::mqtt::decode(&packet, 1024).unwrap()
        else {
            panic!()
        };
        let value: Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(value["data"]["sequence"], i);
        assert_eq!(value["data"]["messageId"], "correlation");
        reply.await.unwrap().unwrap();
    }
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    assert!(peer.read_u8().await.is_err());
}
#[tokio::test(start_paused = true)]
async fn partial_rti_frame_times_out_and_shutdown_cancels_downlink_wait() {
    let (client, mut peer) = duplex(8192);
    let (_up, rx) = mpsc::channel(4);
    let (events, mut received) = mpsc::channel(4);
    let (_stop, stopped) = watch::channel(false);
    let task = tokio::spawn(session::thinq1(client, identity(), rx, events, stopped));
    received.recv().await.unwrap();
    rti(&mut peer).await;
    peer.write_all(&[0, 0, 0, 20, b'{']).await.unwrap();
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    let (client, mut peer) = duplex(8192);
    let (_up, rx) = mpsc::channel(4);
    let (events, mut received) = mpsc::channel(4);
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(session::thinq1(client, identity(), rx, events, stopped));
    received.recv().await.unwrap();
    rti(&mut peer).await;
    peer.write_all(
        &rusthinq_protocol::thinq1::encode(br#"{"Body":{"Cmd":"Ctrl"}}"#, 1024).unwrap(),
    )
    .await
    .unwrap();
    let pending = received.recv().await.unwrap();
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    drop(pending);
    assert!(peer.read_u8().await.is_err());
}
