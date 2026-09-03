//! ThinQ1 HTTPS routes (XML).

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use rusthinq_core::config::Config;
use rusthinq_core::metadata::Metadata;
use rusthinq_util::sync::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

const XML_HEADER: &str = r#"<?xml version="1.0" encoding="utf-8" standalone="yes"?>"#;

pub type MetaStore = Arc<Mutex<HashMap<String, Metadata>>>;

pub fn device_metadata_store() -> MetaStore {
    Arc::new(Mutex::new(HashMap::new()))
}

#[derive(Clone)]
pub struct T1HttpState {
    #[allow(dead_code)] // kept for handlers that need advertise host / ports
    pub config: Arc<Config>,
    pub meta: MetaStore,
}

pub fn routes(config: Arc<Config>, meta: MetaStore) -> Router {
    let state = T1HttpState { config, meta };
    Router::new()
        .route(
            "/lgehadm/api/Device/TotalDeviceInfoSvc",
            post(total_device_info),
        )
        .route("/lgehadm/api/Grid/PowerSavingInfoSvc", post(power_saving))
        .route("/lgehadm/api/Rtos/FWInfoSettingSvc", post(fw_info))
        .route("/lgehadm/report/diagmon", post(diagmon))
        .fallback(fallback)
        .with_state(state)
}

async fn parse_xml_body(body: Bytes) -> Value {
    if body.len() > 1_000_000 {
        return Value::Null;
    }
    let text = String::from_utf8_lossy(&body);
    // Prefer quick-xml if available via serde; fall back to empty.
    // We only need a few fields — do lightweight extraction.
    let mut map = serde_json::Map::new();
    if let Some(model) = extract_tag(&text, "modelName") {
        let mut root = serde_json::Map::new();
        root.insert("modelName".into(), Value::String(model));
        if let Some(item) = extract_tag(&text, "item") {
            let mut item_list = serde_json::Map::new();
            item_list.insert("item".into(), Value::String(item));
            root.insert("itemList".into(), Value::Object(item_list));
        }
        map.insert("lgedmRoot".into(), Value::Object(root));
    }
    Value::Object(map)
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}

fn xml_response(body: &str) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/xml;charset=utf-8")],
        body.to_string(),
    )
        .into_response()
}

async fn total_device_info(
    State(state): State<T1HttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    rusthinq_core::logging::log("HTTPS", &["TotalDeviceInfoSvc"]);
    let parsed = parse_xml_body(body).await;
    let device_id = headers
        .get("x-lgedm-deviceid")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let device_type = headers
        .get("x-lgedm-devicetype")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let Some(device_id) = device_id else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let model_name = parsed
        .pointer("/lgedmRoot/modelName")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if let (Some(model), Some(dt)) = (model_name.clone(), device_type.clone()) {
        state.meta.lock().insert(
            device_id.clone(),
            Metadata {
                model_id: model.clone(),
                model_name: model,
                device_type: Some(dt),
                sw_version: None,
            },
        );
    }

    let item = parsed
        .pointer("/lgedmRoot/itemList/item")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let inner = if item == "DM_SETTING_INFO_GET_URI" {
        r#"<returnCd>0000</returnCd><returnMsg>OK</returnMsg><itemList><elementList><elementCode>settingInfoList</elementCode><elementValueList><code>BlackBox</code><value>N</value></elementValueList></elementList><item>DM_SETTING_INFO_GET_URI</item><returnCode>0000</returnCode></itemList>"#
            .to_string()
    } else if item == "THINQ_TIME_SYNC_URI" {
        let utc = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        format!(
            r#"<returnCd>0000</returnCd><returnMsg>OK</returnMsg><itemList><elementList><elementCode>utcTime</elementCode><elementValue>{utc}</elementValue></elementList><elementList><elementCode>timezone</elementCode><elementValue>0</elementValue></elementList><item>THINQ_TIME_SYNC_URI</item><returnCode>0000</returnCode></itemList>"#
        )
    } else {
        r#"<returnCd>0000</returnCd><returnMsg>OK</returnMsg>"#.to_string()
    };

    xml_response(&format!("{XML_HEADER}<lgedmRoot>{inner}</lgedmRoot>"))
}

async fn power_saving() -> Response {
    xml_response(&format!(
        r#"{XML_HEADER}<lgedmRoot><returnCd>0108</returnCd><returnMsg>No Saving Data.</returnMsg></lgedmRoot>"#
    ))
}

async fn fw_info() -> Response {
    xml_response(&format!(
        r#"{XML_HEADER}<lgedmRoot><returnCd>0000</returnCd><returnMsg>OK</returnMsg></lgedmRoot>"#
    ))
}

async fn diagmon() -> StatusCode {
    StatusCode::OK
}

async fn fallback() -> axum::Json<Value> {
    axum::Json(serde_json::json!({}))
}
