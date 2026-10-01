use rusthinq_protocol::mqtt::{self, Error, Packet};
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
            keep_alive: 60
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
