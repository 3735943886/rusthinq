//! Pure wire/JSON helpers shared by real driver bundles; no device semantics.
use base64::Engine as _;
use rhai::{Array, Blob, Dynamic, Engine, EvalAltResult, Map};
use rusthinq_protocol::{aabb, crc16, hex, tlv};
fn bytes(values: Array) -> Result<Blob, Box<EvalAltResult>> {
    values
        .into_iter()
        .map(|value| {
            value
                .as_int()
                .ok()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| "expected byte array".into())
        })
        .collect()
}
fn items(values: Array) -> Result<Vec<tlv::Tlv>, Box<EvalAltResult>> {
    values
        .into_iter()
        .map(|value| {
            let map = value.try_cast::<Map>().ok_or("expected TLV map")?;
            let t = map
                .get("t")
                .and_then(|v| v.as_int().ok())
                .and_then(|n| u16::try_from(n).ok())
                .filter(|n| *n < 1024)
                .ok_or("invalid TLV tag")?;
            let v = map
                .get("v")
                .and_then(|v| v.as_int().ok())
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| *n < 0x1000000)
                .ok_or("invalid TLV value")?;
            Ok(tlv::Tlv::new(t, v))
        })
        .collect()
}
fn values(items: Vec<tlv::Tlv>) -> Array {
    items
        .into_iter()
        .map(|item| {
            let mut map = Map::new();
            map.insert("t".into(), i64::from(item.t).into());
            map.insert("v".into(), i64::from(item.v).into());
            Dynamic::from_map(map)
        })
        .collect()
}
pub(crate) fn install(engine: &mut Engine) {
    engine.set_max_expr_depths(200, 100);
    engine.register_fn("json_stringify", |map: Map| rhai::format_map_as_json(&map));
    engine.register_fn(
        "json_parse",
        |text: String| -> Result<Dynamic, Box<EvalAltResult>> {
            let value: serde_json::Value =
                serde_json::from_str(&text).map_err(|error| error.to_string())?;
            rhai::serde::to_dynamic(value).map_err(|error| error.to_string().into())
        },
    );
    engine.register_fn(
        "bytes_utf8",
        |bytes: Blob| -> Result<String, Box<EvalAltResult>> {
            String::from_utf8(bytes).map_err(|error| error.to_string().into())
        },
    );
    engine.register_fn(
        "base64_decode",
        |text: String| -> Result<Blob, Box<EvalAltResult>> {
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(|error| error.to_string().into())
        },
    );
    engine.register_fn("base64_encode", |bytes: Blob| {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    });
    engine.register_fn("hex_encode", |bytes: Blob| hex::encode(bytes));
    engine.register_fn("hex_encode_upper", |bytes: Blob| hex::encode_upper(bytes));
    engine.register_fn(
        "hex_decode",
        |text: String| -> Result<Blob, Box<EvalAltResult>> {
            hex::decode(text).map_err(|error| error.to_string().into())
        },
    );
    engine.register_fn("crc16", |bytes: Blob| i64::from(crc16::crc16(&bytes)));
    engine.register_fn(
        "aabb_wrap",
        |bytes: Blob| -> Result<Blob, Box<EvalAltResult>> {
            aabb::wrap(&bytes).ok_or_else(|| "AABB short frame exceeded".into())
        },
    );
    engine.register_fn(
        "aabb_wrap",
        |array: Array| -> Result<Blob, Box<EvalAltResult>> {
            aabb::wrap(&bytes(array)?).ok_or_else(|| "AABB short frame exceeded".into())
        },
    );
    engine.register_fn("aabb_unwrap", |bytes: Blob| match bytes.as_slice() {
        [0xaa, _, inner @ .., _, 0xbb] => Dynamic::from_blob(inner.to_vec()),
        _ => Dynamic::UNIT,
    });
    engine.register_fn("tlv_parse", |bytes: Blob| values(tlv::parse(&bytes)));
    engine.register_fn(
        "tlv_build",
        |array: Array| -> Result<Blob, Box<EvalAltResult>> { Ok(tlv::build(&items(array)?)) },
    );
    engine.register_fn(
        "tlv_frame_build",
        |header: Array, array: Array| -> Result<Blob, Box<EvalAltResult>> {
            tlv::frame_build(&bytes(header)?, &items(array)?)
                .ok_or_else(|| "TLV frame exceeded".into())
        },
    );
    engine.register_fn("tlv_frame_parse", |bytes: Blob| {
        tlv::frame_parse(&bytes).map_or(Dynamic::UNIT, |items| Dynamic::from_array(values(items)))
    });
    engine.register_fn("known_tag_name", |tag: i64| {
        rusthinq_protocol::tlv_catalog::known_tag_name(tag as u16)
            .unwrap_or("")
            .to_string()
    });
    engine.register_fn("is_known_tag", |tag: i64| {
        rusthinq_protocol::tlv_catalog::is_known_tag(tag as u16)
    });
}
