use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use once_cell::sync::Lazy;
use std::sync::RwLock;
use std::error::Error;
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
    jwt_token: String,
    current_offset: RwLock<u64>,
    finished: RwLock<bool>,
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
        println!("Initializing Iggy Client with HTTP REST API...");
        let bind_data = info.get_bind_data::<IggyBindData>();
        let connection_string = unsafe { (*bind_data).server_address.clone() };
        let stream_id = unsafe { (*bind_data).stream_id.clone() };
        let topic_id = unsafe { (*bind_data).topic_id.clone() };
        let partition_id = unsafe { (*bind_data).partition_id };
        
        println!("Connection string: {}", connection_string);

        // Parse to extract host and credentials
        let (http_url, username, password) = parse_iggy_connection_string(&connection_string)?;
        println!("HTTP URL: {}", http_url);

        // Authenticate and get JWT token
        let http_client = Client::new();
        let jwt_token = RUNTIME.block_on(async {
            // The correct login endpoint is /users/login
            let login_url = format!("{}/users/login", http_url);
            println!("Authenticating at: {}", login_url);
            
            let login_body = json!({
                "username": username,
                "password": password
            });
            
            println!("Login request body: {:?}", login_body);
            
            let response = http_client.post(&login_url)
                .header("Content-Type", "application/json")
                .json(&login_body)
                .send()
                .await?;
            
            let status = response.status();
            println!("Login response status: {}", status);
            
            let body_text = response.text().await?;
            println!("Login response body: {}", body_text);
            
            if !body_text.is_empty() {
                let response_json: Value = serde_json::from_str(&body_text)?;
                
                // The response structure is: { "access_token": { "token": "...", "expiry": ... } }
                let token = response_json.get("access_token")
                    .and_then(|obj| obj.get("token"))
                    .and_then(|t| t.as_str())
                    .ok_or("No access_token.token in login response")?
                    .to_string();
                
                println!("Received JWT token: {}...", &token[..token.len().min(20)]);
                Ok::<String, Box<dyn std::error::Error>>(token)
            } else {
                Err("Empty login response body".into())
            }
        })?;

        Ok(IggyInitData {
            http_url,
            stream_id,
            topic_id,
            partition_id,
            http_client,
            jwt_token,
            current_offset: RwLock::new(0),
            finished: RwLock::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        if *init_data.finished.read().unwrap() {
            output.set_len(0);
            return Ok(());
        }

        let json_result = RUNTIME.block_on(async {

            let current_offset = *init_data.current_offset.read().unwrap();
            
            let url = format!(
                "{}/streams/{}/topics/{}/messages?offset={}&count=1024&partition={}",
                init_data.http_url, init_data.stream_id, init_data.topic_id, current_offset, init_data.partition_id
            );
            
            let response = init_data.http_client.get(&url)
                .header("Authorization", format!("Bearer {}", init_data.jwt_token))
                .send()
                .await?;
            
            let body_text = response.text().await?;
            
            let json: serde_json::Value = if body_text.is_empty() {
                serde_json::json!({"messages": []})
            } else {
                serde_json::from_str(&body_text)?
            };
            
            Ok::<serde_json::Value, Box<dyn Error>>(json)
        })?;

        let empty_vec = vec![];
        let messages_array = json_result.get("messages")
            .and_then(|m| m.as_array())
            .unwrap_or(&empty_vec);

        if messages_array.is_empty() {
            let mut finished_guard = init_data.finished.write().unwrap();
            *finished_guard = true;
            output.set_len(0);
            return Ok(());
        }

        let offset_vec = output.flat_vector(0);
        let payload_vec = output.flat_vector(1);

        let count = messages_array.len();
        let mut last_processed_offset = 0;
        let start_offset = *init_data.current_offset.read().unwrap();

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
                use base64::{Engine as _, engine::general_purpose};
       
                let decoded_bytes = general_purpose::STANDARD
                    .decode(payload_base64)
                    .unwrap_or_else(|_| payload_base64.as_bytes().to_vec());

                payload_vec.insert(i, &decoded_bytes);
            }
            
            last_processed_offset = offset;
        }

        {
            let mut offset_guard = init_data.current_offset.write().unwrap();
            *offset_guard = last_processed_offset + 1;
            println!("Cursor updated: Fetching next from offset {}", *offset_guard);
        }

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