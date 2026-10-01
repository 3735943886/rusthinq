use rusthinq_protocol::mqtt::{self, Error, Packet};
#[test]
fn qos2_publish_and_release_require_valid_ids_and_flags() {
    assert!(matches!(
        mqtt::decode(&[0x34, 6, 0, 1, b't', 0, 9, b'x'], 1024),
        Ok(Packet::Publish {
            qos: 2,
            id: Some(9),
            ..
        })
    ));
    assert_eq!(
        mqtt::decode(&[0x62, 2, 0, 9], 1024),
        Ok(Packet::PubRel { id: 9 })
    );
    for packet in [
        &[0x62, 2, 0, 0][..],
        &[0x62, 3, 0, 9, 0],
        &[0x60, 2, 0, 9],
        &[0x36, 6, 0, 1, b't', 0, 9, b'x'],
    ] {
        assert!(mqtt::decode(packet, 1024).is_err());
    }
}
#[test]
fn binary_will_and_flags_are_validated() {
    let body = [
        0, 4, b'M', b'Q', b'T', b'T', 4, 0x2e, 0, 60, 0, 0, 0, 1, b't', 0, 2, 0, 255,
    ];
    assert!(
        matches!(mqtt::decode(&mqtt::frame(0x10, &body, 1024).unwrap(), 1024), Ok(Packet::Connect {will: Some(mqtt::Will {topic, payload, qos:1, retain:true}), ..}) if topic == "t" && payload == [0,255])
    );
    for flags in [0x22, 0x0a, 0x1e] {
        let mut invalid = body;
        invalid[7] = flags;
        assert_eq!(
            mqtt::decode(&mqtt::frame(0x10, &invalid, 1024).unwrap(), 1024),
            Err(Error::Malformed)
        );
    }
}
#[test]
fn bounded_framing_and_packet_validation() {
    let packet = mqtt::publish("clip/message/devices/d", &vec![42; 300], 1024).unwrap();
    for split in 0..3 {
        assert_eq!(mqtt::length(&packet[..split], 1024), Ok(None));
    }
    for split in 3..packet.len() {
        assert!(mqtt::decode(&packet[..split], 1024).is_err());
    }
    assert!(
        matches!(mqtt::decode(&packet, 1024), Ok(Packet::Publish { payload, id: None, .. }) if payload == vec![42; 300])
    );
    assert_eq!(
        mqtt::length(&[0x30, 0xff, 0xff, 0xff, 0xff], 1024),
        Err(Error::Malformed)
    );
    assert_eq!(mqtt::length(&[0x30, 0x80, 0], 1024), Err(Error::Malformed));
    assert_eq!(
        mqtt::length(&[0x30, 0xff, 0x7f], 1024),
        Err(Error::Exceeded)
    );
    assert!(mqtt::decode(&[0xc0, 1, 0], 1024).is_err());
    assert_eq!(mqtt::decode(&[0xc0, 0], 1024), Ok(Packet::Ping));
    let connect = [
        0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 1, b'd',
    ];
    assert_eq!(
        mqtt::decode(&connect, 1024),
        Ok(Packet::Connect {
            client: "d".into(),
            keep_alive: 60,
            will: None
        })
    );
    let mut unsupported = connect;
    unsupported[9] = 0;
    assert_eq!(mqtt::decode(&unsupported, 1024), Err(Error::Unsupported));
    for offset in 0..connect.len() {
        let mut corrupt = connect;
        corrupt[offset] = 255;
        let _ = mqtt::decode(&corrupt, 1024);
    }
}
#[test]
fn subscription_filters_match_levels_and_system_topic_rules() {
    for filter in ["#", "lime/#", "lime/+/d", "lime/devices/d"] {
        assert!(mqtt::valid_filter(filter));
        assert!(mqtt::matches(filter, "lime/devices/d"));
    }
    assert!(mqtt::matches("lime/#", "lime"));
    assert!(!mqtt::matches("lime/+/d", "lime/x/y/d"));
    assert!(!mqtt::matches("#", "$SYS/status"));
    for filter in ["", "x/#/y", "x/a+", "x/a#"] {
        assert!(!mqtt::valid_filter(filter));
    }
}

#[test]
fn qos1_duplicate_flags_and_packet_identifiers_are_validated() {
    let body = [0, 1, b't', 0, 9, b'x'];
    assert_eq!(
        mqtt::decode(&mqtt::frame(0x3a, &body, 1024).unwrap(), 1024),
        Ok(Packet::Publish {
            topic: "t".into(),
            payload: b"x".to_vec(),
            id: Some(9),
            duplicate: true,
            qos: 1,
        })
    );
    let mut zero_id = body;
    zero_id[4] = 0;
    assert_eq!(
        mqtt::decode(&mqtt::frame(0x3a, &zero_id, 1024).unwrap(), 1024),
        Err(Error::Malformed)
    );
    assert_eq!(
        mqtt::decode(&mqtt::frame(0x38, &[0, 1, b't', b'x'], 1024).unwrap(), 1024),
        Err(Error::Malformed)
    );
}

#[test]
fn empty_client_id_is_valid_only_for_supported_clean_sessions() {
    let body = [0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 60, 0, 0];
    assert_eq!(
        mqtt::decode(&mqtt::frame(0x10, &body, 1024).unwrap(), 1024),
        Ok(Packet::Connect {
            client: String::new(),
            keep_alive: 60,
            will: None
        })
    );
    let mut persistent = body;
    persistent[7] = 0;
    assert_eq!(
        mqtt::decode(&mqtt::frame(0x10, &persistent, 1024).unwrap(), 1024),
        Err(Error::Unsupported)
    );
}
