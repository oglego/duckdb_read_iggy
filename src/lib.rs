use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use once_cell::sync::Lazy;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::runtime::Runtime;
use serde_json::{json, Value};
use reqwest::Client;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

/// Parse iggy://[username:password@]host[:port] connection strings into HTTP URL and credentials
fn parse_iggy_connection_string(conn_str: &str) -> Result<(String, String, String), Box<dyn Error>> {
    // Remove iggy:// scheme
    let without_scheme = conn_str
        .strip_prefix("iggy://")
        .unwrap_or(conn_str);
    
    // Extract credentials and host part
    let (credentials, host_port) = if let Some(at_pos) = without_scheme.rfind('@') {
        let creds = &without_scheme[..at_pos];
        let host = &without_scheme[at_pos + 1..];
        (Some(creds), host)
    } else {
        (None, without_scheme)
    };
    
    // Parse username and password
    let (username, password) = if let Some(creds) = credentials {
        if let Some(colon_pos) = creds.find(':') {
            (creds[..colon_pos].to_string(), creds[colon_pos + 1..].to_string())
        } else {
            (creds.to_string(), String::new())
        }
    } else {
        ("iggy".to_string(), "iggy".to_string())
    };
    
    // Parse host (ignore port from connection string, always use 3000 for HTTP)
    let host = if let Some(colon_pos) = host_port.rfind(':') {
        host_port[..colon_pos].to_string()
    } else {
        host_port.to_string()
    };
    
    // HTTP API always uses port 3000
    let http_url = format!("http://{}:3000", host);
    Ok((http_url, username, password))
}

fn extract_access_token(response_json: &Value) -> Result<String, Box<dyn Error>> {
    response_json
        .get("access_token")
        .and_then(|obj| obj.get("token"))
        .and_then(|t| t.as_str())
        .map(str::to_owned)
        .ok_or_else(|| -> Box<dyn Error> { "No access_token.token in login response".into() })
}

fn parse_messages_response(body: &[u8]) -> Result<Value, Box<dyn Error>> {
    if body.is_empty() {
        Ok(json!({ "messages": [] }))
    } else {
        Ok(serde_json::from_slice(body)?)
    }
}

fn decode_payload(payload_base64: &str) -> Vec<u8> {
    use base64::Engine as _;

    base64::engine::general_purpose::STANDARD
        .decode(payload_base64)
        .unwrap_or_else(|_| payload_base64.as_bytes().to_vec())
}

#[repr(C)]
struct IggyBindData {
    stream_id: String,
    topic_id: String,
    partition_id: u32,
    server_address: String,
}

struct IggyInitData {
    http_url: String,
    stream_id: String,
    topic_id: String,
    partition_id: u32,
    http_client: Client,
    auth_header: String,
    current_offset: AtomicU64,
    finished: AtomicBool,
}

struct IggyVTab;

impl VTab for IggyVTab {
    type InitData = IggyInitData;
    type BindData = IggyBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("offset", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("payload", LogicalTypeHandle::from(LogicalTypeId::Blob));

        let stream_param = bind.get_parameter(0).to_string();
        let topic_param = bind.get_parameter(1).to_string();
        let partition_id = bind.get_parameter(2).to_string().parse::<u32>().unwrap_or(1);
        let server_address = bind.get_parameter(3).to_string();

        Ok(IggyBindData {
            stream_id: stream_param,
            topic_id: topic_param,
            partition_id,
            server_address,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<IggyBindData>();
        let connection_string = unsafe { (*bind_data).server_address.clone() };
        let stream_id = unsafe { (*bind_data).stream_id.clone() };
        let topic_id = unsafe { (*bind_data).topic_id.clone() };
        let partition_id = unsafe { (*bind_data).partition_id };

        // Parse to extract host and credentials
        let (http_url, username, password) = parse_iggy_connection_string(&connection_string)?;

        // Authenticate and get JWT token
        let http_client = Client::new();
        let jwt_token = RUNTIME.block_on(async {
            let login_url = format!("{}/users/login", http_url);
            let login_body = json!({
                "username": username,
                "password": password
            });

            let response = http_client.post(&login_url)
                .header("Content-Type", "application/json")
                .json(&login_body)
                .send()
                .await?;

            let response_json: Value = response.json().await?;
            extract_access_token(&response_json)
        })?;

        Ok(IggyInitData {
            http_url,
            stream_id,
            topic_id,
            partition_id,
            http_client,
            auth_header: format!("Bearer {}", jwt_token),
            current_offset: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        if init_data.finished.load(Ordering::Acquire) {
            output.set_len(0);
            return Ok(());
        }

        let json_result = RUNTIME.block_on(async {
            let current_offset = init_data.current_offset.load(Ordering::Relaxed);
            
            let url = format!(
                "{}/streams/{}/topics/{}/messages?offset={}&count=1024&partition={}",
                init_data.http_url, init_data.stream_id, init_data.topic_id, current_offset, init_data.partition_id
            );
            
            let response = init_data.http_client.get(&url)
                .header("Authorization", &init_data.auth_header)
                .send()
                .await?;

            let body = response.bytes().await?;
            let json = parse_messages_response(&body)?;

            Ok::<Value, Box<dyn Error>>(json)
        })?;

        let messages_array = json_result.get("messages")
            .and_then(|m| m.as_array())
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        if messages_array.is_empty() {
            init_data.finished.store(true, Ordering::Release);
            output.set_len(0);
            return Ok(());
        }

        let offset_vec = output.flat_vector(0);
        let payload_vec = output.flat_vector(1);

        let count = messages_array.len();
        let mut last_processed_offset = 0;
        let start_offset = init_data.current_offset.load(Ordering::Relaxed);

        for (i, msg) in messages_array.iter().enumerate() {
            let offset = msg.get("header")
                .and_then(|h| h.get("offset"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            if i == 0 && offset < start_offset && start_offset != 0 {
                output.set_len(0);
                return Ok(());
            }

            unsafe {
                let offset_ptr = offset_vec.as_mut_ptr() as *mut i64;
                *offset_ptr.add(i) = offset as i64;
            }

            if let Some(payload_base64) = msg.get("payload").and_then(|v| v.as_str()) {
                let decoded_bytes = decode_payload(payload_base64);
                payload_vec.insert(i, &decoded_bytes);
            }
            
            last_processed_offset = offset;
        }

        init_data
            .current_offset
            .store(last_processed_offset + 1, Ordering::Release);

        output.set_len(count);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Integer),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<IggyVTab>("read_iggy")
        .expect("Failed to register Iggy consumer");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connection_string_with_credentials_and_port() {
        let (http_url, username, password) =
            parse_iggy_connection_string("iggy://alice:secret@example.com:8090").unwrap();

        assert_eq!(http_url, "http://example.com:3000");
        assert_eq!(username, "alice");
        assert_eq!(password, "secret");
    }

    #[test]
    fn parses_connection_string_without_scheme() {
        let (http_url, username, password) =
            parse_iggy_connection_string("localhost:8090").unwrap();

        assert_eq!(http_url, "http://localhost:3000");
        assert_eq!(username, "iggy");
        assert_eq!(password, "iggy");
    }

    #[test]
    fn parses_connection_string_with_username_only() {
        let (http_url, username, password) =
            parse_iggy_connection_string("iggy://alice@example.com").unwrap();

        assert_eq!(http_url, "http://example.com:3000");
        assert_eq!(username, "alice");
        assert_eq!(password, "");
    }

    #[test]
    fn extracts_access_token_from_login_response() {
        let response_json = json!({
            "access_token": {
                "token": "jwt-token",
                "expiry": 12345
            }
        });

        let token = extract_access_token(&response_json).unwrap();

        assert_eq!(token, "jwt-token");
    }

    #[test]
    fn rejects_login_response_without_nested_token() {
        let response_json = json!({
            "access_token": {
                "expiry": 12345
            }
        });

        let error = extract_access_token(&response_json).unwrap_err();

        assert_eq!(error.to_string(), "No access_token.token in login response");
    }

    #[test]
    fn parses_empty_messages_response() {
        let response_json = parse_messages_response(&[]).unwrap();

        assert_eq!(response_json, json!({ "messages": [] }));
    }

    #[test]
    fn parses_non_empty_messages_response() {
        let response_json = parse_messages_response(
            br#"{"messages":[{"header":{"offset":7},"payload":"SGVsbG8="}]}"#,
        )
        .unwrap();

        assert_eq!(
            response_json["messages"][0]["header"]["offset"].as_u64(),
            Some(7)
        );
        assert_eq!(
            response_json["messages"][0]["payload"].as_str(),
            Some("SGVsbG8=")
        );
    }

    #[test]
    fn decodes_valid_base64_payload() {
        let decoded = decode_payload("SGVsbG8=");

        assert_eq!(decoded, b"Hello");
    }

    #[test]
    fn falls_back_to_raw_bytes_for_invalid_base64_payload() {
        let decoded = decode_payload("not-base64");

        assert_eq!(decoded, b"not-base64");
    }
}
